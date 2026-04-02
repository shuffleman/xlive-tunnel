package rtmp

import (
	"crypto/cipher"
	"net"
	"testing"
	"time"

	xcrypto "github.com/shuffleman/xlive-tunnel/crypto"
)

func TestConnMethodsCoverage(t *testing.T) {
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

	serverCh := make(chan *Server, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		s, err := NewServer(conn, ServerOptions{SelectDecryptor: selectDec})
		if err != nil {
			return
		}
		_, _ = s.Start()
		serverCh <- s
	}()

	cconn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(cconn, ClientOptions{Enc: enc, SessionID: "0123456789abcdef0123456789abcdef"})
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	s := <-serverCh

	_ = client.LocalAddr()
	_ = client.RemoteAddr()
	_ = client.SetDeadline(time.Time{})
	_ = client.SetReadDeadline(time.Time{})
	_ = client.SetWriteDeadline(time.Time{})
	n, err := client.Read(make([]byte, 1))
	if n != 0 || err == nil {
		t.Fatal("expected eof")
	}
	_ = client.Close()
	_ = client.Close()

	_, err = s.Write([]byte("x"))
	if err == nil {
		t.Fatal("expected error")
	}
	_ = s.LocalAddr()
	_ = s.RemoteAddr()
	_ = s.SetDeadline(time.Time{})
	_ = s.SetReadDeadline(time.Time{})
	_ = s.SetWriteDeadline(time.Time{})
	_ = s.Close()
	_ = s.Close()
}
