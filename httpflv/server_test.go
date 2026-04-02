package httpflv

import (
	"bytes"
	"io"
	"testing"
	"time"

	xcrypto "github.com/shuffleman/xlive-tunnel/crypto"
	"github.com/shuffleman/xlive-tunnel/flv"
)

func TestServerStreamAndWriterConn(t *testing.T) {
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

	var buf bytes.Buffer
	stream := NewServerStream(&buf, enc)
	err = stream.Start()
	if err != nil {
		t.Fatal(err)
	}
	wc := NewWriterConn(stream, nil, nil)
	_ = wc.SetDeadline(time.Time{})
	_ = wc.SetReadDeadline(time.Time{})
	_ = wc.SetWriteDeadline(time.Time{})
	_ = wc.LocalAddr()
	_ = wc.RemoteAddr()
	if _, err := wc.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("expected eof, got %v", err)
	}
	want := []byte("payload")
	_, err = wc.Write(want)
	if err != nil {
		t.Fatal(err)
	}
	_ = wc.Close()
	_ = wc.Close()

	data := buf.Bytes()
	r := bytes.NewReader(data)
	_, err = io.ReadAll(io.LimitReader(r, 13))
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = flv.ReadTag(r)
	if err != nil {
		t.Fatal(err)
	}
	_, _, ct, err := flv.ReadTag(r)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, len(ct))
	dec.XORKeyStream(out, ct)
	if !bytes.Equal(out, want) {
		t.Fatal("decrypt mismatch")
	}
}

func TestServerStreamNilEnc(t *testing.T) {
	var buf bytes.Buffer
	stream := NewServerStream(&buf, nil)
	if err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	_, err := stream.Write([]byte("x"))
	if err == nil {
		t.Fatal("expected error")
	}
}
