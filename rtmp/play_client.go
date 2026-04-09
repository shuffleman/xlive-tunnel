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

type PlayClientOptions struct {
	ChunkSize   uint32
	Dec         cipher.Stream
	SessionID   string
	StreamName  string
	Fingerprint *Fingerprint
}

type PlayClient struct {
	raw net.Conn
	c   *Conn

	dec cipher.Stream

	readMu    sync.Mutex
	src       []byte
	srcOff    int
	streamID  uint32
	streamKey string
	fp        Fingerprint
}

var _ net.Conn = (*PlayClient)(nil)

func DialPlay(raw net.Conn, opts PlayClientOptions) (*PlayClient, error) {
	if opts.SessionID == "" {
		return nil, errors.New("rtmp: missing session id")
	}
	if opts.Dec == nil {
		return nil, errors.New("rtmp: nil decryptor")
	}
	if opts.ChunkSize == 0 {
		opts.ChunkSize = 262144
	}
	streamName := opts.StreamName
	if streamName == "" {
		streamName = "live_" + opts.SessionID
	}
	c := newConn(raw)
	c.cw.SetChunkSize(opts.ChunkSize)
	pc := &PlayClient{
		raw:       raw,
		c:         c,
		dec:       opts.Dec,
		streamID:  1,
		streamKey: streamName,
		fp:        normalizeFingerprint(opts.Fingerprint),
	}
	if err := pc.start(opts.SessionID); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return pc, nil
}

func (c *PlayClient) start(sessionID string) error {
	if err := clientHandshake(c.raw); err != nil {
		return err
	}
	_ = c.c.WriteWindowAckSize(2500000)
	_ = c.c.WriteSetPeerBandwidth(2500000, 2)
	_ = c.c.WriteSetChunkSize(c.c.cw.chunkSize)

	if err := c.c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, buildConnectPayloadForConn(sessionID, c.streamKey, c.raw.RemoteAddr(), &c.fp)); err != nil {
		return err
	}
	if _, err := c.waitCommand(amfCmdResult, 1, 5*time.Second); err != nil {
		return err
	}
	if err := c.c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, buildCreateStreamPayload()); err != nil {
		return err
	}
	if _, err := c.waitCommand(amfCmdResult, 2, 5*time.Second); err != nil {
		return err
	}

	b := bytes.NewBuffer(nil)
	amf0WriteString(b, amfCmdPlay)
	amf0WriteNumber(b, 3)
	amf0WriteNull(b)
	amf0WriteString(b, c.streamKey)
	if err := c.c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: c.streamID,
	}, b.Bytes()); err != nil {
		return err
	}

	return nil
}

func (c *PlayClient) waitCommand(name string, txID float64, timeout time.Duration) (*message, error) {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return nil, errors.New("rtmp: command timeout")
		}
		msg, err := c.c.ReadMessage()
		if err != nil {
			return nil, err
		}
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

func (c *PlayClient) Read(p []byte) (n int, err error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	if len(p) == 0 {
		return 0, nil
	}

	for {
		if c.srcOff < len(c.src) {
			n = len(p)
			remain := len(c.src) - c.srcOff
			if remain < n {
				n = remain
			}
			c.dec.XORKeyStream(p[:n], c.src[c.srcOff:c.srcOff+n])
			c.srcOff += n
			if c.srcOff >= len(c.src) {
				c.src = nil
				c.srcOff = 0
			}
			return n, nil
		}

		m, err := c.c.ReadMessage()
		if err != nil {
			return 0, err
		}
		switch m.Header.MessageTypeID {
		case messageTypeSetChunkSize:
			if len(m.Payload) >= 4 {
				size := binary.BigEndian.Uint32(m.Payload[:4])
				c.c.cr.SetChunkSize(size)
			}
		case messageTypeAudio:
			continue
		case messageTypeVideo:
			nalus := parseAVCNALUs(m.Payload)
			if len(nalus) == 0 {
				continue
			}
			var ct []byte
			for _, n := range nalus {
				if b, ok := extractSEIUserDataUnregistered(n); ok && len(b) > 0 {
					ct = append(ct, b...)
				}
			}
			if len(ct) == 0 {
				continue
			}
			c.src = ct
			c.srcOff = 0
		default:
		}
	}
}

func (c *PlayClient) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (c *PlayClient) Close() error {
	err := c.raw.Close()
	c.readMu.Lock()
	c.dec = nil
	c.src = nil
	c.srcOff = 0
	c.c = nil
	c.readMu.Unlock()
	return err
}
func (c *PlayClient) LocalAddr() net.Addr                { return c.raw.LocalAddr() }
func (c *PlayClient) RemoteAddr() net.Addr               { return c.raw.RemoteAddr() }
func (c *PlayClient) SetDeadline(t time.Time) error      { return c.raw.SetDeadline(t) }
func (c *PlayClient) SetReadDeadline(t time.Time) error  { return c.raw.SetReadDeadline(t) }
func (c *PlayClient) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }
