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
	ChunkSize  uint32
	Enc        cipher.Stream
	StreamName string
}

type PlayServer struct {
	raw net.Conn
	c   *Conn

	enc        cipher.Stream
	streamName string

	writeMu         sync.Mutex
	firstWrite      bool
	writeCounter    uint32
	videoFrameCount uint32
	tmp             []byte
	closed          bool
}

var _ net.Conn = (*PlayServer)(nil)

func NewPlayServer(raw net.Conn, opts PlayServerOptions) *PlayServer {
	if opts.ChunkSize == 0 {
		opts.ChunkSize = 262144
	}
	c := newConn(raw)
	c.cw.SetChunkSize(opts.ChunkSize)
	ps := &PlayServer{
		raw:        raw,
		c:          c,
		enc:        opts.Enc,
		streamName: opts.StreamName,
		firstWrite: true,
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
		if perr == nil && name == "connect" {
			break
		}
		_, connectTx, msg, err = s.readCommandAny(5 * time.Second)
		if err != nil {
			return err
		}
	}
	if err := psWriteResultConnect(s.c, connectTx); err != nil {
		return err
	}

	_, createTx, _, err := s.readCommandWait("createStream", 5*time.Second)
	if err != nil {
		return err
	}
	if err := psWriteResultCreateStream(s.c, createTx, 1); err != nil {
		return err
	}

	cmd, _, msg, err := s.readCommandWait("play", 5*time.Second)
	if err != nil {
		return err
	}
	if cmd == "play" && s.streamName == "" {
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
	}, buildSetDataFramePayload()); err != nil {
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
	amf0WriteString(b, "onStatus")
	amf0WriteNumber(b, 0)
	amf0WriteNull(b)
	amf0WriteObject(b, map[string]amf0Value{
		"level":       "status",
		"code":        "NetStream.Play.Start",
		"description": "Start playing",
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

func (s *PlayServer) writeFrame(msgType uint8, csid uint32, hdr []byte, payload []byte) error {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	cw := s.c.cw
	chunkSize := cw.chunkSize
	if cap(s.tmp) < int(chunkSize) {
		s.tmp = make([]byte, chunkSize)
	}
	tmp := s.tmp[:chunkSize]

	totalLen := uint32(len(hdr) + len(payload))
	h := messageHeader{
		Timestamp:       uint32(time.Now().UnixMilli() % 0xFFFFFF),
		MessageTypeID:   msgType,
		MessageStreamID: 1,
	}
	h.MessageLength = totalLen

	remaining := totalLen
	hdrOff := 0
	plainOff := 0
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
		for hdrOff < len(hdr) && cl > 0 {
			n := cl
			if uint32(len(hdr)-hdrOff) < n {
				n = uint32(len(hdr) - hdrOff)
			}
			if _, err := cw.w.Write(hdr[hdrOff : hdrOff+int(n)]); err != nil {
				return err
			}
			hdrOff += int(n)
			cl -= n
			remaining -= n
		}
		if cl > 0 && plainOff < len(payload) {
			n := int(cl)
			if len(payload)-plainOff < n {
				n = len(payload) - plainOff
			}
			s.enc.XORKeyStream(tmp[:n], payload[plainOff:plainOff+n])
			if _, err := cw.w.Write(tmp[:n]); err != nil {
				return err
			}
			plainOff += n
			remaining -= uint32(n)
		}
	}
	if s.c.bw != nil {
		return s.c.bw.Flush()
	}
	return nil
}

func (s *PlayServer) Write(p []byte) (n int, err error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.enc == nil {
		return 0, errors.New("rtmp: nil encryptor")
	}
	if len(p) == 0 {
		return 0, nil
	}
	var hdr []byte
	var msgType uint8
	var csid uint32
	if s.firstWrite {
		s.firstWrite = false
		hdr = []byte{aacSoundFormat, aacPacketRaw}
		msgType = messageTypeAudio
		csid = csidAudio
	} else {
		s.writeCounter++
		if s.writeCounter%10 == 0 {
			hdr = []byte{aacSoundFormat, aacPacketRaw}
			msgType = messageTypeAudio
			csid = csidAudio
		} else {
			frameType := byte(avcFrameInter)
			s.videoFrameCount++
			if s.videoFrameCount%60 == 0 {
				frameType = avcFrameKeyframe
			}
			hdr = make([]byte, 9)
			hdr[0] = frameType
			hdr[1] = avcPacketNALU
			binary.BigEndian.PutUint32(hdr[5:9], uint32(len(p)))
			msgType = messageTypeVideo
			csid = csidVideo
		}
	}
	if err := s.writeFrame(msgType, csid, hdr, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *PlayServer) Read(p []byte) (int, error)         { return 0, io.EOF }
func (s *PlayServer) Close() error                       { s.closed = true; return s.raw.Close() }
func (s *PlayServer) LocalAddr() net.Addr                { return s.raw.LocalAddr() }
func (s *PlayServer) RemoteAddr() net.Addr               { return s.raw.RemoteAddr() }
func (s *PlayServer) SetDeadline(t time.Time) error      { return s.raw.SetDeadline(t) }
func (s *PlayServer) SetReadDeadline(t time.Time) error  { return s.raw.SetReadDeadline(t) }
func (s *PlayServer) SetWriteDeadline(t time.Time) error { return s.raw.SetWriteDeadline(t) }

func psWriteResultConnect(c *Conn, txID float64) error {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, "_result")
	amf0WriteNumber(b, txID)
	amf0WriteObject(b, map[string]amf0Value{
		"fmsVer":       "FMS/3,0,1,123",
		"capabilities": 31.0,
		"mode":         1.0,
	})
	amf0WriteObject(b, map[string]amf0Value{
		"level":          "status",
		"code":           "NetConnection.Connect.Success",
		"description":    "Connection succeeded.",
		"clientid":       "NGINX RTMP (github.com/sergey-dryabzhinsky/nginx-rtmp-module)",
		"objectEncoding": 0.0,
	})
	return c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, b.Bytes())
}

func psWriteResultCreateStream(c *Conn, txID float64, streamID uint32) error {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, "_result")
	amf0WriteNumber(b, txID)
	amf0WriteNull(b)
	amf0WriteNumber(b, float64(streamID))
	return c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, b.Bytes())
}
