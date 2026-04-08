package rtmp

import (
	"bytes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

type PlayServerOptions struct {
	ChunkSize   uint32
	Enc         cipher.Stream
	StreamName  string
	Fingerprint *Fingerprint
}

type PlayServer struct {
	raw net.Conn
	c   *Conn

	enc        cipher.Stream
	streamName string
	fp         Fingerprint

	writeMu         sync.Mutex
	videoFrameCount uint32
	audioTS         uint32
	videoTS         uint32
	dataIn          chan []byte
	tmp             []byte
	closed          bool
	closeCh         chan struct{}
}

var _ net.Conn = (*PlayServer)(nil)

func NewPlayServer(raw net.Conn, opts PlayServerOptions) *PlayServer {
	if opts.ChunkSize == 0 {
		opts.ChunkSize = 4096
	}
	c := newConn(raw)
	c.cw.SetChunkSize(opts.ChunkSize)
	ps := &PlayServer{
		raw:        raw,
		c:          c,
		enc:        opts.Enc,
		streamName: opts.StreamName,
		fp:         normalizeFingerprint(opts.Fingerprint),
		dataIn:     make(chan []byte, 256),
		closeCh:    make(chan struct{}),
	}
	return ps
}

func (s *PlayServer) Start() error {
	if err := serverHandshake(s.raw); err != nil {
		return err
	}
	_ = s.c.WriteWindowAckSize(2500000)
	_ = s.c.WriteSetPeerBandwidth(2500000, 2)
	_ = s.c.WriteSetChunkSize(s.c.cw.chunkSize)

	_, connectTx, msg, err := s.readCommandAny(5 * time.Second)
	if err != nil {
		return err
	}
	for msg != nil {
		name, _, perr := parseCommandNameAndTxID(msg.Payload)
		if perr == nil && name == amfCmdConnect {
			break
		}
		_, connectTx, msg, err = s.readCommandAny(5 * time.Second)
		if err != nil {
			return err
		}
	}
	if err := psWriteResultConnect(s.c, connectTx, &s.fp); err != nil {
		return err
	}

	_, createTx, _, err := s.readCommandWait(amfCmdCreateStream, 5*time.Second)
	if err != nil {
		return err
	}
	if err := psWriteResultCreateStream(s.c, createTx, 1); err != nil {
		return err
	}

	cmd, _, msg, err := s.readCommandWait(amfCmdPlay, 5*time.Second)
	if err != nil {
		return err
	}
	if cmd == amfCmdPlay && s.streamName == "" {
		dec := newAMF0Decoder(msg.Payload)
		_, _ = dec.readValue()
		_, _ = dec.readValue()
		_, _ = dec.readValue()
		if v, e := dec.readValue(); e == nil {
			if sn, ok := v.(string); ok && sn != "" {
				s.streamName = sn
			}
		}
	}
	if err := s.writeOnStatusPlayStart(); err != nil {
		return err
	}
	if err := s.c.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 1,
	}, buildSetDataFramePayload(&s.fp)); err != nil {
		return err
	}
	if err := s.c.writeRawMessage(csidAudio, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeAudio,
		MessageStreamID: 1,
	}, buildAACSeqHdrMessage()); err != nil {
		return err
	}
	if err := s.c.writeRawMessage(csidVideo, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeVideo,
		MessageStreamID: 1,
	}, buildAVCSeqHdrMessage()); err != nil {
		return err
	}
	go s.pacer()
	return nil
}

func (s *PlayServer) StreamName() string {
	return s.streamName
}

func (s *PlayServer) SetEnc(enc cipher.Stream) {
	s.enc = enc
}

func (s *PlayServer) writeOnStatusPlayStart() error {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, amfCmdOnStatus)
	amf0WriteNumber(b, 0)
	amf0WriteNull(b)
	amf0WriteObject(b, map[string]amf0Value{
		"level":       amfLevelStatus,
		"code":        amfCodeNetStreamPlayStart,
		"description": amfDescStartPlaying,
	})
	return s.c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 1,
	}, b.Bytes())
}

func (s *PlayServer) readCommandAny(timeout time.Duration) (name string, txID float64, msg *message, err error) {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return "", 0, nil, errors.New("rtmp: command timeout")
		}
		msg, err = s.c.ReadMessage()
		if err != nil {
			return "", 0, nil, err
		}
		if msg.Header.MessageTypeID == messageTypeSetChunkSize && len(msg.Payload) >= 4 {
			size := binary.BigEndian.Uint32(msg.Payload[:4])
			s.c.cr.SetChunkSize(size)
			continue
		}
		if msg.Header.MessageTypeID != messageTypeCommandAMF0 {
			continue
		}
		name, txID, err = parseCommandNameAndTxID(msg.Payload)
		if err != nil {
			continue
		}
		return name, txID, msg, nil
	}
}

func (s *PlayServer) readCommandWait(name string, timeout time.Duration) (cmd string, txID float64, msg *message, err error) {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return "", 0, nil, errors.New("rtmp: command timeout")
		}
		cmd, txID, msg, err = s.readCommandAny(time.Until(deadline))
		if err != nil {
			return "", 0, nil, err
		}
		if cmd == name {
			return cmd, txID, msg, nil
		}
	}
}

func (s *PlayServer) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	select {
	case <-s.closeCh:
		return 0, io.ErrClosedPipe
	case s.dataIn <- cp:
		return len(p), nil
	}
}

func (s *PlayServer) pacer() {
	audioTicker := time.NewTicker(23 * time.Millisecond)
	videoTicker := time.NewTicker(33 * time.Millisecond)
	defer audioTicker.Stop()
	defer videoTicker.Stop()

	var pending []byte
	for {
		select {
		case <-s.closeCh:
			return
		case b := <-s.dataIn:
			pending = append(pending, b...)
		case <-audioTicker.C:
			_ = s.writeAudioFrame()
			s.audioTS += 23
		case <-videoTicker.C:
			_ = s.writeVideoFrame(&pending)
			s.videoTS += 33
		}
	}
}

func (s *PlayServer) writeAudioFrame() error {
	body := make([]byte, 2+len(sampleAACRaw))
	body[0] = aacSoundFormat
	body[1] = aacPacketRaw
	copy(body[2:], sampleAACRaw)
	return s.writeMessage(s.audioTS, messageTypeAudio, csidAudio, body)
}

func (s *PlayServer) writeVideoFrame(pending *[]byte) error {
	s.videoFrameCount++
	frameType := byte(avcFrameInter)
	base := sampleP
	if s.videoFrameCount%60 == 0 {
		frameType = avcFrameKeyframe
		base = sampleIDR
	}

	var nalus [][]byte
	nalus = append(nalus, base)

	if s.enc != nil && len(*pending) > 0 {
		maxCipher := s.maxCipherPerVideoFrame(len(base))
		if maxCipher > len(*pending) {
			maxCipher = len(*pending)
		}
		if maxCipher > 0 {
			ct := make([]byte, maxCipher)
			s.enc.XORKeyStream(ct, (*pending)[:maxCipher])
			*pending = (*pending)[maxCipher:]
			nalus = append(nalus, buildSEIUserDataUnregistered(ct))
		}
	}

	body := buildAVCVideoBody(frameType, nalus...)
	target := s.targetVideoBodySize()
	if pad := target - len(body); pad > 0 {
		filler := buildFillerNALU(pad - 4)
		body = buildAVCVideoBody(frameType, append(nalus, filler)...)
	}

	return s.writeMessage(s.videoTS, messageTypeVideo, csidVideo, body)
}

func (s *PlayServer) targetVideoBodySize() int {
	dr := s.fp.Meta.VideoDataRate
	fr := s.fp.Meta.FrameRate
	if dr <= 0 || fr <= 0 {
		return 5 + 4 + len(sampleP)
	}
	bytesPerSec := int(dr * 1000 / 8)
	bytesPerFrame := bytesPerSec / int(fr)
	if bytesPerFrame < 512 {
		bytesPerFrame = 512
	}
	return 5 + bytesPerFrame
}

func (s *PlayServer) maxCipherPerVideoFrame(baseNALULen int) int {
	target := s.targetVideoBodySize()
	overhead := 5 + (4 + baseNALULen)
	seiOverhead := 4 + 1 + 2 + 16 + 1
	max := target - overhead - seiOverhead
	if max < 0 {
		return 0
	}
	return max
}

func (s *PlayServer) writeMessage(ts uint32, msgType uint8, csid uint32, body []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.c == nil {
		return io.ErrClosedPipe
	}
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	cw := s.c.cw
	chunkSize := cw.chunkSize
	if cap(s.tmp) < int(chunkSize) {
		s.tmp = make([]byte, chunkSize)
	}

	totalLen := uint32(len(body))
	h := messageHeader{
		Timestamp:       ts,
		MessageTypeID:   msgType,
		MessageStreamID: 1,
	}
	h.MessageLength = totalLen

	remaining := totalLen
	first := true
	for remaining > 0 {
		cl := chunkSize
		if remaining < cl {
			cl = remaining
		}
		if first {
			if err := writeBasicHeader(cw.w, 0, csid); err != nil {
				return err
			}
			if err := writeMessageHeader(cw.w, 0, h); err != nil {
				return err
			}
			first = false
		} else {
			if err := writeBasicHeader(cw.w, 3, csid); err != nil {
				return err
			}
		}
		n := int(cl)
		off := int(totalLen - remaining)
		if _, err := cw.w.Write(body[off : off+n]); err != nil {
			return err
		}
		remaining -= uint32(n)
	}
	if s.c.bw != nil {
		return s.c.bw.Flush()
	}
	return nil
}

func (s *PlayServer) Read(p []byte) (int, error) { return 0, io.EOF }
func (s *PlayServer) Close() error {
	s.closed = true
	select {
	case <-s.closeCh:
	default:
		close(s.closeCh)
	}
	err := s.raw.Close()
	s.writeMu.Lock()
	s.enc = nil
	s.tmp = nil
	s.c = nil
	s.writeMu.Unlock()
	return err
}
func (s *PlayServer) LocalAddr() net.Addr                { return s.raw.LocalAddr() }
func (s *PlayServer) RemoteAddr() net.Addr               { return s.raw.RemoteAddr() }
func (s *PlayServer) SetDeadline(t time.Time) error      { return s.raw.SetDeadline(t) }
func (s *PlayServer) SetReadDeadline(t time.Time) error  { return s.raw.SetReadDeadline(t) }
func (s *PlayServer) SetWriteDeadline(t time.Time) error { return s.raw.SetWriteDeadline(t) }

func psWriteResultConnect(c *Conn, txID float64, fp *Fingerprint) error {
	nfp := normalizeFingerprint(fp)
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, amfCmdResult)
	amf0WriteNumber(b, txID)
	amf0WriteObject(b, map[string]amf0Value{
		"fmsVer":       nfp.ServerFmsVer,
		"capabilities": 31.0,
		"mode":         1.0,
	})
	amf0WriteObject(b, map[string]amf0Value{
		"level":          amfLevelStatus,
		"code":           amfCodeNetConnectionConnectSuccess,
		"description":    amfDescConnectionSucceeded,
		"clientid":       nfp.ServerClientID,
		"objectEncoding": 0.0,
	})
	return c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, b.Bytes())
}

func psWriteResultCreateStream(c *Conn, txID float64, streamID uint32) error {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, amfCmdResult)
	amf0WriteNumber(b, txID)
	amf0WriteNull(b)
	amf0WriteNumber(b, float64(streamID))
	return c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, b.Bytes())
}
