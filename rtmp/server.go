package rtmp

import (
	"bytes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type ServerOptions struct {
	ChunkSize       uint32
	SelectDecryptor func(firstCiphertext []byte) (cipher.Stream, error)
	Fingerprint     *Fingerprint
}

type ServerSession struct {
	ID string
}

type Server struct {
	raw net.Conn
	c   *Conn

	dec       cipher.Stream
	selectDec func(firstCiphertext []byte) (cipher.Stream, error)

	mu         sync.Mutex
	plainPipeR *io.PipeReader
	plainPipeW *io.PipeWriter
	dataCh     chan []byte
	sessionID  string
	xliveReady chan struct{}
	doneCh     chan struct{}
	drainOnce  sync.Once

	// relay mode: when key detection fails, forward raw RTMP messages
	// to an upstream server (e.g., nginx-rtmp) instead of decrypting.
	relayMu    sync.Mutex
	relayMode  bool
	relayCh    chan RelayMessage
	relayReady chan struct{}

	bytesReceived uint32
	xliveExpected bool

	windowAckSize uint32
	lastAck       uint32
	closeOnce     sync.Once
	closeCh       chan struct{}

	fp Fingerprint
}

var _ net.Conn = (*Server)(nil)

func NewServer(raw net.Conn, opts ServerOptions) (*Server, error) {
	if opts.ChunkSize == 0 {
		// Use nginx-rtmp default chunk size for fingerprint disguise
		opts.ChunkSize = 4096
	}
	if opts.SelectDecryptor == nil {
		return nil, errors.New("rtmp: missing decryptor selector")
	}
	c := newConn(raw)
	// Only set chunkWriter — our send chunk size.
	// chunkReader stays at RTMP default (128) until the remote peer
	// sends its own SetChunkSize, which is handled in readLoop.
	c.cw.SetChunkSize(opts.ChunkSize)
	pr, pw := io.Pipe()
	s := &Server{
		raw:           raw,
		c:             c,
		selectDec:     opts.SelectDecryptor,
		plainPipeR:    pr,
		plainPipeW:    pw,
		dataCh:        make(chan []byte, 256),
		xliveReady:    make(chan struct{}),
		doneCh:        make(chan struct{}),
		relayCh:       make(chan RelayMessage, 256),
		relayReady:    make(chan struct{}),
		windowAckSize: 2500000,
		closeCh:       make(chan struct{}),
		fp:            normalizeFingerprint(opts.Fingerprint),
	}
	return s, nil
}

func (s *Server) drainLoop() {
	defer s.plainPipeW.Close()
	for {
		select {
		case <-s.closeCh:
			return
		case data, ok := <-s.dataCh:
			if !ok {
				return
			}
			_, err := s.plainPipeW.Write(data)
			if err != nil {
				return
			}
		}
	}
}

func (s *Server) Start() (sessionID string, err error) {
	err = serverHandshake(s.raw)
	if err != nil {
		return "", fmt.Errorf("handshake: %w", err)
	}

	// Use nginx-rtmp default values for fingerprint disguise
	_ = s.c.WriteWindowAckSize(2500000)
	_ = s.c.WriteSetPeerBandwidth(2500000, 2)
	_ = s.c.WriteSetChunkSize(s.c.cw.chunkSize)

	_, connectTx, msg, err := s.readCommandAny(5 * time.Second)
	if err != nil {
		return "", fmt.Errorf("read connect: %w", err)
	}
	for msg != nil {
		cmd, _, err := parseCommandNameAndTxID(msg.Payload)
		if err == nil && cmd == amfCmdConnect {
			break
		}
		_, connectTx, msg, err = s.readCommandAny(5 * time.Second)
		if err != nil {
			return "", fmt.Errorf("read connect loop: %w", err)
		}
	}
	if msg == nil {
		return "", errors.New("rtmp: missing connect")
	}
	sid, err := extractSessionIDFromConnect(msg.Payload)
	if err != nil {
		return "", fmt.Errorf("extract session: %w", err)
	}
	s.sessionID = sid
	err = s.writeResultConnect(connectTx)
	if err != nil {
		return "", fmt.Errorf("write connect result: %w", err)
	}

	_, createTx, _, err := s.readCommandWait(amfCmdCreateStream, 5*time.Second)
	if err != nil {
		return "", fmt.Errorf("read createStream: %w", err)
	}
	err = s.writeResultCreateStream(createTx, 1)
	if err != nil {
		return "", fmt.Errorf("write createStream result: %w", err)
	}

	_, _, publishMsg, _ := s.readCommandWait(amfCmdPublish, 5*time.Second)
	if s.sessionID == "" && publishMsg != nil {
		if streamName, e := extractStreamNameFromPublish(publishMsg.Payload); e == nil {
			if sid := extractSessionIDFromStreamName(streamName); sid != "" {
				s.sessionID = sid
			} else {
				s.sessionID = streamName
			}
		}
	}
	s.xliveExpected = isLikelySessionID(s.sessionID)
	_ = s.writeOnStatusPublishStart()

	s.drainOnce.Do(func() { go s.drainLoop() })
	go s.readLoop()
	return s.sessionID, nil
}

func (s *Server) readCommandAny(timeout time.Duration) (name string, txID float64, msg *message, err error) {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return "", 0, nil, errors.New("rtmp: command timeout")
		}
		msg, err := s.c.ReadMessage()
		if err != nil {
			return "", 0, nil, fmt.Errorf("ReadMessage error: %w", err)
		}
		// Sync chunkReader when client notifies its send chunk size
		if msg.Header.MessageTypeID == messageTypeSetChunkSize && len(msg.Payload) >= 4 {
			size := binary.BigEndian.Uint32(msg.Payload[:4])
			s.c.cr.SetChunkSize(size)
			continue
		}
		if msg.Header.MessageTypeID != messageTypeCommandAMF0 {
			continue
		}
		cmd, id, err := parseCommandNameAndTxID(msg.Payload)
		if err != nil {
			return "", 0, nil, fmt.Errorf("parseCommand from %d-byte payload (msgLen=%d): %w", len(msg.Payload), msg.Header.MessageLength, err)
		}
		return cmd, id, msg, nil
	}
}

func (s *Server) readCommandWait(name string, timeout time.Duration) (cmd string, txID float64, msg *message, err error) {
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

func (s *Server) writeResultConnect(txID float64) error {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, amfCmdResult)
	amf0WriteNumber(b, txID)
	// Properties object — matches nginx-rtmp ngx_rtmp_cmd_module.c
	amf0WriteObject(b, map[string]amf0Value{
		"fmsVer":       s.fp.ServerFmsVer,
		"capabilities": 31.0,
		"mode":         1.0,
	})
	// Information object — matches nginx-rtmp _result response
	amf0WriteObject(b, map[string]amf0Value{
		"level":          amfLevelStatus,
		"code":           amfCodeNetConnectionConnectSuccess,
		"description":    amfDescConnectionSucceeded,
		"clientid":       s.fp.ServerClientID,
		"objectEncoding": 0.0,
	})
	return s.c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, b.Bytes())
}

func (s *Server) writeResultCreateStream(txID float64, streamID uint32) error {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, amfCmdResult)
	amf0WriteNumber(b, txID)
	amf0WriteNull(b)
	amf0WriteNumber(b, float64(streamID))
	return s.c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, b.Bytes())
}

func (s *Server) writeOnStatusPublishStart() error {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, amfCmdOnStatus)
	amf0WriteNumber(b, 0)
	amf0WriteNull(b)
	// Matches nginx-rtmp ngx_rtmp_cmd_module.c: ngx_rtmp_cmd_publish_response
	amf0WriteObject(b, map[string]amf0Value{
		"level":       amfLevelStatus,
		"code":        amfCodeNetStreamPublishStart,
		"description": amfDescStartPublishing,
		"clientid":    s.fp.ServerClientID,
	})
	return s.c.writeRawMessage(csidCommand, messageHeader{
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 1,
	}, b.Bytes())
}

// readLoop reads RTMP messages. In normal mode, it handles dual-stream
// decryption. In relay mode, it forwards raw messages to relayCh.
func (s *Server) readLoop() {
	defer close(s.doneCh)
	defer close(s.dataCh)
	defer func() {
		s.relayMu.Lock()
		if s.relayMode {
			close(s.relayCh)
		}
		s.relayMu.Unlock()
	}()

	for {
		msg, err := s.c.ReadMessage()
		if err != nil {
			return
		}
		s.bytesReceived += uint32(len(msg.Payload))
		if s.windowAckSize > 0 && s.bytesReceived-s.lastAck >= s.windowAckSize {
			_ = s.c.WriteAcknowledgement(s.bytesReceived)
			s.lastAck = s.bytesReceived
		}

		// Relay mode: forward raw messages to upstream
		s.relayMu.Lock()
		isRelay := s.relayMode
		s.relayMu.Unlock()
		if isRelay {
			switch msg.Header.MessageTypeID {
			case messageTypeSetChunkSize:
				if len(msg.Payload) >= 4 {
					size := binary.BigEndian.Uint32(msg.Payload[:4])
					s.c.cr.SetChunkSize(size)
				}
			case messageTypeAudio, messageTypeVideo:
				select {
				case s.relayCh <- RelayMessage{
					ChunkStreamID: msg.ChunkStreamID,
					Header:        msg.Header,
					Payload:       msg.Payload,
				}:
				default:
					// drop if relay channel full
				}
			}
			continue
		}

		// Normal mode: dual-stream decryption
		switch msg.Header.MessageTypeID {
		case messageTypeSetChunkSize:
			if len(msg.Payload) >= 4 {
				size := binary.BigEndian.Uint32(msg.Payload[:4])
				s.c.cr.SetChunkSize(size)
			}
		case messageTypeAudio:
			s.handleAudioMessage(msg)
		case messageTypeVideo:
			s.handleVideoMessage(msg)
		default:
		}
	}
}

// enterRelayMode switches the server from normal mode to relay mode.
// Called when key detection fails (real RTMP stream detected).
func (s *Server) enterRelayMode() {
	s.relayMu.Lock()
	defer s.relayMu.Unlock()
	if !s.relayMode {
		s.relayMode = true
		close(s.relayReady)
	}
}

// handleAudioMessage processes Audio messages (AAC format).
func (s *Server) handleAudioMessage(msg *message) {
	if s.xliveExpected {
		return
	}
	if len(msg.Payload) < 2 {
		return
	}

	soundFmt := msg.Payload[0]
	if soundFmt != aacSoundFormat && soundFmt != 0xAE {
		return
	}

	pktType := msg.Payload[1]
	switch pktType {
	case aacPacketSeqHdr:
		return
	case aacPacketRaw:
	default:
		return
	}

	data := msg.Payload[2:]

	if s.dec == nil {
		dec, err := s.selectDec(data)
		if err != nil {
			// Key detection failed — enter relay mode
			s.enterRelayMode()
			return
		}
		s.dec = dec
		select {
		case <-s.xliveReady:
		default:
			close(s.xliveReady)
		}
	}

	s.dec.XORKeyStream(data, data)
	select {
	case s.dataCh <- data:
	case <-s.closeCh:
	}
}

// handleVideoMessage processes Video messages (H.264 AVC format).
func (s *Server) handleVideoMessage(msg *message) {
	if len(msg.Payload) < 2 {
		return
	}

	pktType := msg.Payload[1]
	switch pktType {
	case avcPacketSeqHdr:
		return
	case avcPacketNALU:
	default:
		return
	}

	nalus := parseAVCNALUs(msg.Payload)
	if len(nalus) == 0 {
		return
	}

	var extracted [][]byte
	for _, n := range nalus {
		if ct, ok := extractSEIUserDataUnregistered(n); ok && len(ct) > 0 {
			extracted = append(extracted, ct)
		}
	}
	if len(extracted) == 0 {
		return
	}

	if s.dec == nil {
		dec, err := s.selectDec(extracted[0])
		if err != nil {
			if !s.xliveExpected {
				s.enterRelayMode()
			}
			return
		}
		s.dec = dec
		select {
		case <-s.xliveReady:
		default:
			close(s.xliveReady)
		}
	}

	for _, ct := range extracted {
		pt := make([]byte, len(ct))
		copy(pt, ct)
		s.dec.XORKeyStream(pt, pt)
		select {
		case s.dataCh <- pt:
		case <-s.closeCh:
			return
		}
	}
}

// XLIVEReady returns a channel that is closed when the first encrypted data frame
// is received and the decryptor is successfully initialized (normal xlive mode).
func (s *Server) XLIVEReady() <-chan struct{} {
	return s.xliveReady
}

// RelayReady returns a channel that is closed when key detection fails and
// the server enters relay mode (forwarding to upstream).
func (s *Server) RelayReady() <-chan struct{} {
	return s.relayReady
}

// RelayCh returns the channel of raw RTMP messages for relay forwarding.
// Only valid after RelayReady() is signaled.
func (s *Server) RelayCh() <-chan RelayMessage {
	return s.relayCh
}

func (s *Server) Done() <-chan struct{} {
	return s.doneCh
}

func (s *Server) Read(p []byte) (n int, err error) {
	return s.plainPipeR.Read(p)
}

func (s *Server) Write(p []byte) (n int, err error) {
	return 0, errors.New("rtmp: server side write unsupported")
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		close(s.closeCh)
		_ = s.plainPipeW.Close()
		_ = s.raw.Close()
	})
	return nil
}

func (s *Server) LocalAddr() net.Addr  { return s.raw.LocalAddr() }
func (s *Server) RemoteAddr() net.Addr { return s.raw.RemoteAddr() }
func (s *Server) SetDeadline(t time.Time) error {
	return s.raw.SetDeadline(t)
}
func (s *Server) SetReadDeadline(t time.Time) error {
	return s.raw.SetReadDeadline(t)
}
func (s *Server) SetWriteDeadline(t time.Time) error {
	return s.raw.SetWriteDeadline(t)
}
