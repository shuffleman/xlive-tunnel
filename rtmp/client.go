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
	ChunkSize  uint32
	Enc        cipher.Stream
	SessionID  string
	StreamName string
}

type Client struct {
	raw net.Conn
	c   *Conn

	enc        cipher.Stream
	sid        string
	streamName string

	writeMu         sync.Mutex
	firstWrite      bool
	writeCounter    uint32
	videoFrameCount uint32

	readOnce sync.Once
	readErr  error
	closeCh  chan struct{}

	tmp []byte
}

var _ net.Conn = (*Client)(nil)

func NewClient(raw net.Conn, opts ClientOptions) *Client {
	if opts.ChunkSize == 0 {
		opts.ChunkSize = 262144
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
		closeCh:    make(chan struct{}),
		firstWrite: true,
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
	}, buildConnectPayload(c.sid))
	if err != nil {
		return err
	}
	_, err = c.waitCommand("_result", 1, 5*time.Second)
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
	_, err = c.waitCommand("_result", 2, 5*time.Second)
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
	}, buildSetDataFramePayload())
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
	for {
		_, err := c.c.ReadMessage()
		if err != nil {
			c.readOnce.Do(func() { c.readErr = err })
			return
		}
	}
}

// Write sends proxy data as interleaved Video (H.264) and Audio (AAC) frames.
// The first write always goes through Audio to allow server-side key detection
// (the VLESS header naturally provides the 0x00 + UUID probe pattern).
// Subsequent writes alternate at ~90% Video / ~10% Audio ratio.
func (c *Client) Write(p []byte) (n int, err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	if c.enc == nil {
		return 0, errors.New("rtmp: nil encryptor")
	}

	maxPayload := DefaultMaxFramePayload
	if maxPayload < 1024 {
		maxPayload = 1024
	}
	if maxPayload > 4*1024*1024 {
		maxPayload = 4 * 1024 * 1024
	}

	off := 0
	for off < len(p) {
		end := off + maxPayload
		if end > len(p) {
			end = len(p)
		}
		wn, werr := c.writeOnce(p[off:end])
		n += wn
		if werr != nil {
			return n, werr
		}
		off = end
	}
	return n, nil
}

func (c *Client) writeOnce(p []byte) (n int, err error) {
	var hdr []byte
	var msgType uint8
	var csid uint32

	if c.firstWrite {
		// First write MUST be Audio for server key detection.
		// The encrypted VLESS header (0x00 + UUID + ...) serves as the probe.
		c.firstWrite = false
		hdr = []byte{aacSoundFormat, aacPacketRaw} // 0xAF 0x01
		msgType = messageTypeAudio
		csid = csidAudio
	} else {
		c.writeCounter++
		if c.writeCounter%10 == 0 {
			// ~10% of writes go to Audio
			hdr = []byte{aacSoundFormat, aacPacketRaw}
			msgType = messageTypeAudio
			csid = csidAudio
		} else {
			// ~90% of writes go to Video
			frameType := byte(avcFrameInter)
			c.videoFrameCount++
			// Keyframe every ~60 video frames (~2 seconds at 30fps)
			if c.videoFrameCount%60 == 0 {
				frameType = avcFrameKeyframe
			}
			// AVC NALU header: [frameType|codecID][avcPacketType=NALU][compTime 3B][NALU length 4B]
			var hb [9]byte
			hb[0] = frameType
			hb[1] = avcPacketNALU
			// hdr[2:5] = compositionTime = 0 (no B-frames)
			binary.BigEndian.PutUint32(hb[5:9], uint32(len(p)))
			hdr = hb[:]
			msgType = messageTypeVideo
			csid = csidVideo
		}
	}

	err = c.writeFrame(msgType, csid, hdr, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// writeFrame writes a generic RTMP message with codec header + encrypted payload,
// handling chunked transmission with on-the-fly CFB encryption.
func (c *Client) writeFrame(msgType uint8, csid uint32, hdr []byte, payload []byte) error {
	c.c.mu.Lock()
	defer c.c.mu.Unlock()

	cw := c.c.cw
	chunkSize := cw.chunkSize
	if uint32(len(c.tmp)) < chunkSize {
		c.tmp = make([]byte, chunkSize)
	}

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

		// Write plaintext codec header bytes first
		for hdrOff < len(hdr) && chunkLen > 0 {
			n := chunkLen
			if uint32(len(hdr)-hdrOff) < n {
				n = uint32(len(hdr) - hdrOff)
			}
			if _, err := cw.w.Write(hdr[hdrOff : hdrOff+int(n)]); err != nil {
				return err
			}
			hdrOff += int(n)
			chunkLen -= n
			remaining -= n
		}

		// Write encrypted payload data
		if chunkLen > 0 && plainOff < len(payload) {
			n := int(chunkLen)
			if len(payload)-plainOff < n {
				n = len(payload) - plainOff
			}
			c.enc.XORKeyStream(c.tmp[:n], payload[plainOff:plainOff+n])
			if _, err := cw.w.Write(c.tmp[:n]); err != nil {
				return err
			}
			plainOff += n
			remaining -= uint32(n)
		}
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
	c.tmp = nil
	c.c = nil
	c.writeMu.Unlock()
	return err
}

func (c *Client) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (c *Client) LocalAddr() net.Addr                { return c.raw.LocalAddr() }
func (c *Client) RemoteAddr() net.Addr               { return c.raw.RemoteAddr() }
func (c *Client) SetDeadline(t time.Time) error       { return c.raw.SetDeadline(t) }
func (c *Client) SetReadDeadline(t time.Time) error   { return c.raw.SetReadDeadline(t) }
func (c *Client) SetWriteDeadline(t time.Time) error  { return c.raw.SetWriteDeadline(t) }

