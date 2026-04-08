package rtmp

import (
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"time"
)

// RelayMessage represents a parsed RTMP message to be forwarded.
type RelayMessage struct {
	ChunkStreamID uint32
	Header        messageHeader
	Payload       []byte
}

// RelayClient connects to an upstream RTMP server (e.g., nginx-rtmp) and
// forwards audio/video messages. It reuses the existing RTMP protocol stack
// (handshake, chunk framing, AMF0 encoding).
type RelayClient struct {
	raw net.Conn
	c   *Conn
}

// NewRelayClient establishes a TCP connection to the upstream RTMP server.
func NewRelayClient(address string) (*RelayClient, error) {
	conn, err := net.DialTimeout("tcp", address, 10*time.Second)
	if err != nil {
		return nil, err
	}
	c := newConn(conn)
	// Start with nginx-rtmp default chunk size
	c.SetChunkSize(4096)
	return &RelayClient{raw: conn, c: c}, nil
}

// Connect performs the full RTMP publish handshake with the upstream server:
// handshake → connect → createStream → publish.
// streamKey is the publish point name (e.g., "live/mystream").
func (r *RelayClient) Connect(streamKey string) error {
	err := clientHandshake(r.raw)
	if err != nil {
		return err
	}

	_ = r.c.WriteWindowAckSize(2500000)
	_ = r.c.WriteSetPeerBandwidth(2500000, 2)
	_ = r.c.WriteSetChunkSize(r.c.cw.chunkSize)

	// connect
	app := defaultRTMPAppBase
	if a, _, ok := strings.Cut(streamKey, "/"); ok && a != "" {
		app = sanitizeRTMPName(a)
		if app == "" {
			app = defaultRTMPAppBase
		}
	}
	fp := DefaultFingerprint()
	fp.AppBase = app
	host := hostFromAddr(r.raw.RemoteAddr())
	if host == "" {
		host = fp.TcURLHost
	}
	tcURL := fp.TcURLScheme + host + "/" + app
	err = r.c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, buildConnectPayloadWithAppAndTcURL(app, tcURL, fp))
	if err != nil {
		return err
	}
	_, err = r.waitCommand(amfCmdResult, 1, 5*time.Second)
	if err != nil {
		return err
	}

	// createStream
	err = r.c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, buildCreateStreamPayload())
	if err != nil {
		return err
	}
	_, err = r.waitCommand(amfCmdResult, 2, 5*time.Second)
	if err != nil {
		return err
	}

	// publish
	err = r.c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 1,
	}, buildPublishPayload(streamKey))
	if err != nil {
		return err
	}

	go r.drainLoop()
	return nil
}

// drainLoop reads and discards incoming messages from the upstream server
// (e.g., onStatus, windowAck). Keeps the connection alive.
func (r *RelayClient) drainLoop() {
	for {
		msg, err := r.c.ReadMessage()
		if err != nil {
			return
		}
		// Sync chunkReader when upstream changes its send chunk size
		if msg.Header.MessageTypeID == messageTypeSetChunkSize && len(msg.Payload) >= 4 {
			size := binary.BigEndian.Uint32(msg.Payload[:4])
			r.c.cr.SetChunkSize(size)
		}
	}
}

// Forward writes a relay message to the upstream server with proper RTMP chunk framing.
func (r *RelayClient) Forward(msg RelayMessage) error {
	return r.c.writeRawMessage(msg.ChunkStreamID, msg.Header, msg.Payload)
}

func (r *RelayClient) waitCommand(name string, txID float64, timeout time.Duration) (*message, error) {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return nil, errors.New("rtmp: relay command timeout")
		}
		msg, err := r.c.ReadMessage()
		if err != nil {
			return nil, err
		}
		// Sync chunkReader when upstream changes its send chunk size
		if msg.Header.MessageTypeID == messageTypeSetChunkSize && len(msg.Payload) >= 4 {
			size := binary.BigEndian.Uint32(msg.Payload[:4])
			r.c.cr.SetChunkSize(size)
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

// Close shuts down the relay connection.
func (r *RelayClient) Close() error {
	return r.raw.Close()
}
