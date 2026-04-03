package rtmp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
)

const (
	messageTypeSetChunkSize     = 1
	messageTypeAcknowledgement  = 3
	messageTypeUserControl      = 4
	messageTypeWindowAckSize    = 5
	messageTypeSetPeerBandwidth = 6
	messageTypeCommandAMF0      = 20
	messageTypeAudio            = 8
	messageTypeVideo            = 9
)

const (
	csidControl = 2
	csidCommand = 3
	csidAudio   = 4
	csidVideo   = 5
)

type Conn struct {
	c  net.Conn
	cr *chunkReader
	cw *chunkWriter
	bw *bufio.Writer

	mu sync.Mutex
}

var DefaultIOBufferSize = 4 * 1024
var DefaultMaxFramePayload = 64 * 1024

func newConn(c net.Conn) *Conn {
	size := DefaultIOBufferSize
	if size < 4096 {
		size = 4096
	}
	br := bufio.NewReaderSize(c, size)
	bw := bufio.NewWriterSize(c, size)
	return &Conn{
		c:  c,
		cr: newChunkReader(br),
		cw: newChunkWriter(bw),
		bw: bw,
	}
}

func (c *Conn) SetChunkSize(size uint32) {
	c.cr.SetChunkSize(size)
	c.cw.SetChunkSize(size)
}

func (c *Conn) writeRawMessage(csid uint32, h messageHeader, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.cw.WriteMessage(csid, h, payload)
	if err != nil {
		return err
	}
	if c.bw != nil {
		return c.bw.Flush()
	}
	return nil
}

func (c *Conn) WriteSetChunkSize(size uint32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], size)
	return c.writeRawMessage(csidControl, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeSetChunkSize,
		MessageStreamID: 0,
	}, b[:])
}

func (c *Conn) WriteWindowAckSize(size uint32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], size)
	return c.writeRawMessage(csidControl, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeWindowAckSize,
		MessageStreamID: 0,
	}, b[:])
}

func (c *Conn) WriteSetPeerBandwidth(size uint32, limitType byte) error {
	var b [5]byte
	binary.BigEndian.PutUint32(b[:4], size)
	b[4] = limitType
	return c.writeRawMessage(csidControl, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeSetPeerBandwidth,
		MessageStreamID: 0,
	}, b[:])
}

func (c *Conn) WriteAcknowledgement(sequence uint32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], sequence)
	return c.writeRawMessage(csidControl, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeAcknowledgement,
		MessageStreamID: 0,
	}, b[:])
}

func (c *Conn) ReadMessage() (*message, error) {
	return c.cr.ReadMessage()
}

func buildConnectPayload(sessionID string) []byte {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, "connect")
	amf0WriteNumber(b, 1)
	amf0WriteObject(b, map[string]amf0Value{
		"app":            "live/" + sessionID,
		"flashVer":       "FMLE/3.0 (compatible; FMSc/1.0)",
		"tcUrl":          "rtmp://localhost/live/" + sessionID,
		"fpad":           false,
		"capabilities":   15,
		"audioCodecs":    3575,
		"videoCodecs":    252,
		"videoFunction":  1,
		"objectEncoding": 0,
	})
	return b.Bytes()
}

func buildCreateStreamPayload() []byte {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, "createStream")
	amf0WriteNumber(b, 2)
	amf0WriteNull(b)
	return b.Bytes()
}

func buildFCPublishPayload(streamName string) []byte {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, "FCPublish")
	amf0WriteNumber(b, 3)
	amf0WriteNull(b)
	amf0WriteString(b, streamName)
	return b.Bytes()
}

func buildReleaseStreamPayload(streamName string) []byte {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, "releaseStream")
	amf0WriteNumber(b, 2)
	amf0WriteNull(b)
	amf0WriteString(b, streamName)
	return b.Bytes()
}

func buildPublishPayload(streamName string) []byte {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, "publish")
	amf0WriteNumber(b, 3)
	amf0WriteNull(b)
	amf0WriteString(b, streamName)
	amf0WriteString(b, "live")
	return b.Bytes()
}

func parseCommandNameAndTxID(payload []byte) (name string, txID float64, err error) {
	d := newAMF0Decoder(payload)
	v0, err := d.readValue()
	if err != nil {
		return "", 0, err
	}
	name, _ = v0.(string)
	v1, err := d.readValue()
	if err != nil {
		return "", 0, err
	}
	txID, _ = v1.(float64)
	return name, txID, nil
}

// buildSetDataFramePayload builds the @setDataFrame onMetaData message.
// Declares stream properties: 1920x1080 H.264 30fps 3Mbps + AAC 44100Hz 160kbps.
// Real RTMP servers parse this to configure transcoders and recording.
func buildSetDataFramePayload() []byte {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, "@setDataFrame")
	amf0WriteString(b, "onMetaData")
	amf0WriteECMAArray(b, map[string]amf0Value{
		"duration":        0.0,
		"width":           1920.0,
		"height":          1080.0,
		"videodatarate":   3000.0,
		"framerate":       30.0,
		"videocodecid":    7.0,
		"audiodatarate":   160.0,
		"audiosamplerate": 44100.0,
		"audiosamplesize": 16.0,
		"stereo":          true,
		"audiocodecid":    10.0,
		"encoder":         "OBS Server",
		"fileSize":        0.0,
	})
	return b.Bytes()
}

func extractSessionIDFromConnect(payload []byte) (string, error) {
	d := newAMF0Decoder(payload)
	v0, err := d.readValue()
	if err != nil {
		return "", fmt.Errorf("read value 0 at offset %d: %w", d.i, err)
	}
	_, _ = v0.(string)
	v1, err := d.readValue()
	if err != nil {
		return "", fmt.Errorf("read value 1 at offset %d: %w", d.i, err)
	}
	_, _ = v1.(float64)
	v2, err := d.readValue()
	if err != nil {
		return "", fmt.Errorf("read value 2 at offset %d/%d: %w", d.i, len(payload), err)
	}
	obj, _ := v2.(map[string]amf0Value)
	app, _ := obj["app"].(string)
	if app == "" {
		return "", errors.New("rtmp: missing app")
	}
	for i := len(app) - 1; i >= 0; i-- {
		if app[i] == '/' {
			return app[i+1:], nil
		}
	}
	return app, nil
}
