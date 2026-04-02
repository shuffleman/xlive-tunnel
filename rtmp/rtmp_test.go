package rtmp

import (
	"crypto/cipher"
	"testing"

	"net"

	xcrypto "github.com/shuffleman/xlive-tunnel/crypto"
)

func TestRTMPUploadAckRetry(t *testing.T) {
	shared := make([]byte, 16)
	keyiv, err := xcrypto.DeriveKeyIV(shared)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := xcrypto.NewCFBEncrypter(keyiv)
	if err != nil {
		t.Fatal(err)
	}
	selectDec := func([]byte) (cipher.Stream, error) {
		return xcrypto.NewCFBDecrypter(keyiv)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	type serverStart struct {
		sid    string
		server *Server
		err    error
	}
	started := make(chan serverStart, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			started <- serverStart{err: err}
			return
		}
		s, err := NewServer(conn, ServerOptions{
			ChunkSize:       1024,
			SelectDecryptor: selectDec,
		})
		if err != nil {
			started <- serverStart{err: err}
			return
		}
		sid, err := s.Start()
		started <- serverStart{sid: sid, server: s, err: err}
	}()

	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	client := NewClient(c1, ClientOptions{
		ChunkSize: 262144,
		Enc:       enc,
		SessionID: "0123456789abcdef0123456789abcdef",
	})
	err = client.Start()
	if err != nil {
		t.Fatal(err)
	}
	ss := <-started
	if ss.err != nil {
		t.Fatal(ss.err)
	}
	server := ss.server

	payload := []byte("hello-xlive")
	_, err = client.Write(payload)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(payload))
	_, err = server.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != string(payload) {
		t.Fatalf("mismatch: got=%q want=%q", string(buf), string(payload))
	}
}
