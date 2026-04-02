package httpflv

import (
	"bytes"
	"io"
	"net"
	"net/http/httputil"
	"testing"
	"time"

	xcrypto "github.com/xlive-project/xlive/crypto"
)

func TestHTTPFLVEncryptDecrypt(t *testing.T) {
	shared := make([]byte, 16)
	keyiv, err := xcrypto.DeriveKeyIV(shared)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := xcrypto.NewCFBEncrypter(keyiv)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := xcrypto.NewCFBDecrypter(keyiv)
	if err != nil {
		t.Fatal(err)
	}

	want := bytes.Repeat([]byte("xlive"), 1024)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: video/x-flv\r\nTransfer-Encoding: chunked\r\n\r\n"))
		cw := httputil.NewChunkedWriter(conn)
		defer cw.Close()
		stream := NewServerStream(cw, enc)
		_ = stream.Start()
		_, _ = stream.Write(want)
		<-done
		_ = conn.Close()
	}()

	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	cc, err := Dial(c1, ClientOptions{
		Path: "/live/abc.flv?token={sid}",
		Host: "example.com",
		SID:  "0123456789abcdef0123456789abcdef",
		Dec:  dec,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	got := make([]byte, len(want))
	_, err = io.ReadFull(cc, got)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cc.Write([]byte("x"))
	if err != io.ErrClosedPipe {
		t.Fatalf("expected closed pipe, got %v", err)
	}
	_ = cc.SetDeadline(time.Time{})
	_ = cc.SetReadDeadline(time.Time{})
	_ = cc.SetWriteDeadline(time.Time{})
	close(done)
	if !bytes.Equal(got, want) {
		t.Fatal("payload mismatch")
	}
}
