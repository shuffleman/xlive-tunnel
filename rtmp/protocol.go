package rtmp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
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
	fp := DefaultFingerprint()
	app := sanitizeRTMPName(fp.AppBase)
	tcURL := fp.TcURLScheme + fp.TcURLHost + "/" + app
	return buildConnectPayloadWithAppAndTcURL(app, tcURL, fp)
}

func buildConnectPayloadForConn(sessionID string, streamName string, remote net.Addr, fp *Fingerprint) []byte {
	nfp := normalizeFingerprint(fp)
	app := buildConnectAppBaseFromStreamName(sessionID, streamName, &nfp)
	host := hostFromAddr(remote)
	if nfp.TcURLHost != "" && nfp.TcURLHost != defaultTcURLHost {
		host = nfp.TcURLHost
	} else if host == "" {
		host = nfp.TcURLHost
	}
	return buildConnectPayloadWithAppAndTcURL(app, nfp.TcURLScheme+host+"/"+app, nfp)
}

func buildConnectPayloadWithAppAndTcURL(app string, tcURL string, fp Fingerprint) []byte {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, amfCmdConnect)
	amf0WriteNumber(b, 1)
	amf0WriteObject(b, map[string]amf0Value{
		"app":            app,
		"flashVer":       fp.FlashVer,
		"tcUrl":          tcURL,
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
	amf0WriteString(b, amfCmdCreateStream)
	amf0WriteNumber(b, 2)
	amf0WriteNull(b)
	return b.Bytes()
}

func buildFCPublishPayload(streamName string) []byte {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, amfCmdFCPublish)
	amf0WriteNumber(b, 3)
	amf0WriteNull(b)
	amf0WriteString(b, streamName)
	return b.Bytes()
}

func buildReleaseStreamPayload(streamName string) []byte {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, amfCmdReleaseStream)
	amf0WriteNumber(b, 2)
	amf0WriteNull(b)
	amf0WriteString(b, streamName)
	return b.Bytes()
}

func buildPublishPayload(streamName string) []byte {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, amfCmdPublish)
	amf0WriteNumber(b, 3)
	amf0WriteNull(b)
	amf0WriteString(b, streamName)
	amf0WriteString(b, rtmpPublishTypeLive)
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

func extractStreamNameFromPublish(payload []byte) (string, error) {
	d := newAMF0Decoder(payload)
	v0, err := d.readValue()
	if err != nil {
		return "", err
	}
	name, _ := v0.(string)
	if name != amfCmdPublish {
		return "", errors.New("rtmp: not publish")
	}
	if _, err := d.readValue(); err != nil {
		return "", err
	}
	if _, err := d.readValue(); err != nil {
		return "", err
	}
	v3, err := d.readValue()
	if err != nil {
		return "", err
	}
	streamName, _ := v3.(string)
	if streamName == "" {
		return "", errors.New("rtmp: missing stream name")
	}
	return streamName, nil
}

func extractSessionIDFromStreamName(streamName string) string {
	i := strings.LastIndexByte(streamName, '_')
	if i < 0 || i+1 >= len(streamName) {
		return ""
	}
	sid := streamName[i+1:]
	if isLikelySessionID(sid) {
		return sid
	}
	return ""
}

// buildSetDataFramePayload builds the @setDataFrame onMetaData message.
// Declares stream properties: 1920x1080 H.264 30fps 3Mbps + AAC 44100Hz 160kbps.
// Real RTMP servers parse this to configure transcoders and recording.
func buildSetDataFramePayload(fp *Fingerprint) []byte {
	nfp := normalizeFingerprint(fp)
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, "@setDataFrame")
	amf0WriteString(b, "onMetaData")
	amf0WriteECMAArray(b, map[string]amf0Value{
		"duration":        0.0,
		"width":           nfp.Meta.Width,
		"height":          nfp.Meta.Height,
		"videodatarate":   nfp.Meta.VideoDataRate,
		"framerate":       nfp.Meta.FrameRate,
		"videocodecid":    nfp.Meta.VideoCodecID,
		"audiodatarate":   nfp.Meta.AudioDataRate,
		"audiosamplerate": nfp.Meta.AudioSampleRate,
		"audiosamplesize": nfp.Meta.AudioSampleSize,
		"stereo":          nfp.Meta.Stereo,
		"audiocodecid":    nfp.Meta.AudioCodecID,
		"encoder":         nfp.Encoder,
		"fileSize":        0.0,
	})
	return b.Bytes()
}

func buildConnectAppFromStreamName(sessionID string, streamName string, fp *Fingerprint) string {
	nfp := normalizeFingerprint(fp)
	base := nfp.AppBase
	if sessionID != "" && streamName != "" {
		if trimmed, ok := strings.CutSuffix(streamName, "_"+sessionID); ok && trimmed != "" {
			base = trimmed
		} else if i := strings.IndexByte(streamName, '_'); i > 0 {
			base = streamName[:i]
		}
	}
	base = sanitizeRTMPName(base)
	if base == "" {
		base = nfp.AppBase
	}
	return base
}

func buildConnectAppBaseFromStreamName(sessionID string, streamName string, fp *Fingerprint) string {
	return buildConnectAppFromStreamName(sessionID, streamName, fp)
}

func hostFromAddr(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	s := addr.String()
	if host, _, err := net.SplitHostPort(s); err == nil {
		return host
	}
	return s
}

func sanitizeRTMPName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('_')
	}
	return strings.Trim(b.String(), "_")
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
			sid := app[i+1:]
			if isLikelySessionID(sid) {
				return sid, nil
			}
			return "", nil
		}
	}
	if isLikelySessionID(app) {
		return app, nil
	}
	return "", nil
}

func isLikelySessionID(s string) bool {
	if len(s) == 32 {
		for i := 0; i < len(s); i++ {
			c := s[i]
			if c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' {
				continue
			}
			return false
		}
		return true
	}
	if len(s) == 36 && strings.Count(s, "-") == 4 {
		return true
	}
	return false
}
