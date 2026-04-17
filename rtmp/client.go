package rtmp

import (
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

type ClientOptions struct {
	ChunkSize   uint32
	Enc         cipher.Stream
	SessionID   string
	StreamName  string
	Fingerprint *Fingerprint
}

type Client struct {
	raw net.Conn
	c   *Conn

	enc        cipher.Stream
	sid        string
	streamName string
	fp         Fingerprint

	writeMu         sync.Mutex
	videoFrameCount uint32
	audioTS         uint32
	videoTS         uint32
	dataIn          chan []byte

	readOnce sync.Once
	readErr  error
	closeCh  chan struct{}
}

var _ net.Conn = (*Client)(nil)

func NewClient(raw net.Conn, opts ClientOptions) *Client {
	if opts.ChunkSize == 0 {
		opts.ChunkSize = 4096
	}
	streamName := opts.StreamName
	if streamName == "" {
		streamName = "live_" + opts.SessionID
	}
	c := newConn(raw)
	// Only set chunkWriter — our send chunk size.
	// chunkReader stays at RTMP default (128) until the server sends its SetChunkSize.
	c.cw.SetChunkSize(opts.ChunkSize)
	return &Client{
		raw:        raw,
		c:          c,
		enc:        opts.Enc,
		sid:        opts.SessionID,
		streamName: streamName,
		fp:         normalizeFingerprint(opts.Fingerprint),
		closeCh:    make(chan struct{}),
		dataIn:     make(chan []byte, 256),
	}
}

func (c *Client) Start() error {
	err := clientHandshake(c.raw)
	if err != nil {
		return err
	}

	// Mimic OBS/FMLE client handshake parameters
	_ = c.c.WriteWindowAckSize(2500000)
	_ = c.c.WriteSetPeerBandwidth(2500000, 2)
	_ = c.c.WriteSetChunkSize(c.c.cw.chunkSize)
	// chunkReader stays at RTMP default (128) until the server sends its SetChunkSize.
	// Handled in waitCommand.

	err = c.c.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, buildConnectPayloadForConn(c.sid, c.streamName, c.raw.RemoteAddr(), &c.fp))
	if err != nil {
		return err
	}
	_, err = c.waitCommand(amfCmdResult, 1, 5*time.Second)
	if err != nil {
		return err
	}

	err = c.c.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, buildReleaseStreamPayload(c.streamName))
	if err != nil {
		return err
	}

	err = c.c.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, buildFCPublishPayload(c.streamName))
	if err != nil {
		return err
	}

	err = c.c.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, buildCreateStreamPayload())
	if err != nil {
		return err
	}
	_, err = c.waitCommand(amfCmdResult, 2, 5*time.Second)
	if err != nil {
		return err
	}

	err = c.c.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 1,
	}, buildPublishPayload(c.streamName))
	if err != nil {
		return err
	}

	err = c.c.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 1,
	}, buildSetDataFramePayload(&c.fp))
	if err != nil {
		return err
	}

	// AAC sequence header — sent before data frames, parsed by real RTMP servers
	// to identify the audio codec configuration
	err = c.c.writeRawMessage(csidAudio, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeAudio,
		MessageStreamID: 1,
	}, buildAACSeqHdrMessage())
	if err != nil {
		return err
	}

	// AVC sequence header — contains SPS/PPS for H.264, parsed by real RTMP servers
	// to negotiate video codec. Once accepted, subsequent NALU frames are forwarded as-is.
	err = c.c.writeRawMessage(csidVideo, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeVideo,
		MessageStreamID: 1,
	}, buildAVCSeqHdrMessage())
	if err != nil {
		return err
	}

	go c.pacer()
	go c.readLoop()
	return nil
}

func (c *Client) SessionID() string {
	if c == nil {
		return ""
	}
	return c.sid
}

func (c *Client) waitCommand(name string, txID float64, timeout time.Duration) (*message, error) {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return nil, errors.New("rtmp: command timeout")
		}
		msg, err := c.c.ReadMessage()
		if err != nil {
			return nil, err
		}
		// Sync chunkReader when server notifies its send chunk size
		if msg.Header.MessageTypeID == messageTypeSetChunkSize && len(msg.Payload) >= 4 {
			size := binary.BigEndian.Uint32(msg.Payload[:4])
			c.c.cr.SetChunkSize(size)
			continue
		}
		if msg.Header.MessageTypeID != messageTypeCommandAMF0 {
			continue
		}
		cmd, id, err := parseCommandNameAndTxID(msg.Payload)
		if err != nil {
			continue
		}
		if cmd == name && id == txID {
			return msg, nil
		}
	}
}

func (c *Client) readLoop() {
	c.writeMu.Lock()
	cc := c.c
	c.writeMu.Unlock()
	if cc == nil {
		return
	}
	for {
		_, err := cc.ReadMessage()
		if err != nil {
			c.readOnce.Do(func() { c.readErr = err })
			return
		}
	}
}

func (c *Client) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.writeMu.Lock()
	encOK := c.enc != nil
	c.writeMu.Unlock()
	if !encOK {
		return 0, errors.New("rtmp: nil encryptor")
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	select {
	case <-c.closeCh:
		return 0, io.ErrClosedPipe
	case c.dataIn <- cp:
		return len(p), nil
	}
}

const maxPendingSize = 4 << 20 // 4MB: stop reading dataIn when pending exceeds this

func (c *Client) pacer() {
	audioTicker := time.NewTicker(23 * time.Millisecond)
	videoTicker := time.NewTicker(33 * time.Millisecond)
	defer audioTicker.Stop()
	defer videoTicker.Stop()

	var pending []byte
	for {
		// When pending is too large, stop reading from dataIn so that
		// dataIn fills up and Write() blocks (natural backpressure).
		// Only drain pending via video frames until it shrinks.
		if len(pending) > maxPendingSize {
			select {
			case <-c.closeCh:
				return
			case <-audioTicker.C:
				_ = c.writeAudioFrame()
				c.audioTS += 23
			case <-videoTicker.C:
				_ = c.writeVideoFrame(&pending)
				c.videoTS += 33
			}
			continue
		}
		select {
		case <-c.closeCh:
			return
		case b := <-c.dataIn:
			pending = append(pending, b...)
		case <-audioTicker.C:
			_ = c.writeAudioFrame()
			c.audioTS += 23
		case <-videoTicker.C:
			_ = c.writeVideoFrame(&pending)
			c.videoTS += 33
		}
	}
}

func (c *Client) writeAudioFrame() error {
	return c.writeMessage(c.audioTS, messageTypeAudio, csidAudio, sampleAACBody)
}

func (c *Client) writeVideoFrame(pending *[]byte) error {
	c.videoFrameCount++
	frameType := byte(avcFrameInter)
	base := sampleP
	if c.videoFrameCount%60 == 0 {
		frameType = avcFrameKeyframe
		base = sampleIDR
	}

	if len(*pending) == 0 {
		if frameType == avcFrameKeyframe {
			return c.writeMessage(c.videoTS, messageTypeVideo, csidVideo, sampleVideoBodyIDR)
		}
		return c.writeMessage(c.videoTS, messageTypeVideo, csidVideo, sampleVideoBodyP)
	}

	nalus := [][]byte{base}
	if len(*pending) > 0 {
		target := c.targetVideoBodySize()
		maxCipher := c.maxCipherForTarget(target, len(base))
		if maxCipher > len(*pending) {
			maxCipher = len(*pending)
		}
		if maxCipher > 0 {
			ct := make([]byte, maxCipher)
			copy(ct, (*pending)[:maxCipher])
			*pending = (*pending)[maxCipher:]
			nalus = append(nalus, buildSEIUserDataUnregistered(ct))
		}
	}
	body := buildAVCVideoBody(frameType, nalus...)
	return c.writeMessage(c.videoTS, messageTypeVideo, csidVideo, body)
}

func (c *Client) targetVideoBodySize() int {
	dr := c.fp.Meta.VideoDataRate
	fr := c.fp.Meta.FrameRate
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

func (c *Client) maxCipherForTarget(target int, baseNALULen int) int {
	if target <= 0 {
		return 0
	}
	max := target - baseNALULen - 64
	if max <= 0 {
		return 0
	}
	for i := 0; i < 4; i++ {
		payloadLen := 16 + max
		sizeFieldLen := payloadLen/255 + 1
		need := baseNALULen + max + sizeFieldLen + 64
		if need <= target {
			return max
		}
		max -= need - target
		if max <= 0 {
			return 0
		}
	}
	return max
}

func (c *Client) writeMessage(ts uint32, msgType uint8, csid uint32, body []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.c == nil {
		return io.ErrClosedPipe
	}
	c.c.mu.Lock()
	defer c.c.mu.Unlock()

	cw := c.c.cw
	chunkSize := cw.chunkSize

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
		chunkLen := chunkSize
		if remaining < chunkLen {
			chunkLen = remaining
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

		n := int(chunkLen)
		off := int(totalLen - remaining)
		if _, err := cw.w.Write(body[off : off+n]); err != nil {
			return err
		}
		remaining -= uint32(n)
	}

	if c.c.bw != nil {
		return c.c.bw.Flush()
	}
	return nil
}

func (c *Client) Close() error {
	select {
	case <-c.closeCh:
	default:
		close(c.closeCh)
	}
	err := c.raw.Close()
	c.writeMu.Lock()
	c.enc = nil
	c.c = nil
	c.writeMu.Unlock()
	return err
}

func (c *Client) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (c *Client) LocalAddr() net.Addr                { return c.raw.LocalAddr() }
func (c *Client) RemoteAddr() net.Addr               { return c.raw.RemoteAddr() }
func (c *Client) SetDeadline(t time.Time) error      { return c.raw.SetDeadline(t) }
func (c *Client) SetReadDeadline(t time.Time) error  { return c.raw.SetReadDeadline(t) }
func (c *Client) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }
