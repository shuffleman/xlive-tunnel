package rtmp

import (
	"crypto/aes"
	"crypto/cipher"
	"net"
	"testing"
	"time"

	xcrypto "github.com/xlive-project/xlive/crypto"
)

func TestAcceptRealRTMPPublish(t *testing.T) {
	shared := make([]byte, 16)
	keyiv, err := xcrypto.DeriveKeyIV(shared)
	if err != nil {
		t.Fatal(err)
	}

	// Mock selectDecryptor that validates the probe pattern like the real one:
	// first decrypted byte must be 0x00 for xlive mode.
	block, _ := aes.NewCipher(keyiv.Key[:])
	realDec := cipher.NewCFBDecrypter(block, keyiv.IV[:])

	selectDec := func(firstCiphertext []byte) (cipher.Stream, error) {
		if len(firstCiphertext) < 1 {
			return nil, errNoMatch
		}
		probe := make([]byte, 1)
		realDec.XORKeyStream(probe, firstCiphertext[:1])
		if probe[0] != 0x00 {
			return nil, errNoMatch
		}
		// Reset and return a fresh decryptor
		return xcrypto.NewCFBDecrypter(keyiv)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverCh := make(chan *Server, 1)
	go func() {
		conn, _ := ln.Accept()
		s, _ := NewServer(conn, ServerOptions{SelectDecryptor: selectDec})
		_, _ = s.Start()
		serverCh <- s
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = writeC0C1(c, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	s1, err := readS0S1S2(c)
	if err != nil {
		t.Fatal(err)
	}
	err = writeC2(c, s1)
	if err != nil {
		t.Fatal(err)
	}

	cc := newConn(c)
	cc.SetChunkSize(4096)
	_ = cc.WriteSetChunkSize(4096)

	err = cc.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, buildConnectPayload("live"))
	if err != nil {
		t.Fatal(err)
	}
	err = cc.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, buildCreateStreamPayload())
	if err != nil {
		t.Fatal(err)
	}
	err = cc.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 1,
	}, buildPublishPayload("test"))
	if err != nil {
		t.Fatal(err)
	}

	// Send random audio data (0x02 0x03) that won't match xlive probe (0x00 + key)
	err = cc.writeRawMessage(csidAudio, messageHeader{
		Timestamp:       1,
		MessageTypeID:   messageTypeAudio,
		MessageStreamID: 1,
	}, []byte{0xAF, 0x01, 0x02, 0x03})
	if err != nil {
		t.Fatal(err)
	}

	s := <-serverCh
	select {
	case <-s.XLIVEReady():
		t.Fatal("unexpected xlive ready for real rtmp")
	case <-s.RelayReady():
		// Expected: key detection failed → relay mode
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected relay mode to activate")
	}
	_ = s.Close()
}

// errNoMatch is returned by the mock selectDecryptor when probe doesn't match.
var errNoMatch = errNoMatchType{}

type errNoMatchType struct{}

func (errNoMatchType) Error() string { return "no matching key" }
