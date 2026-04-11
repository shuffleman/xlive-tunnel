package httpflv

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/shuffleman/xlive-tunnel/flv"
)

func TestServerStreamAndWriterConn(t *testing.T) {
	var buf bytes.Buffer
	stream := NewServerStream(&buf, nil)
	err := stream.Start()
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
	if !bytes.Equal(ct, want) {
		t.Fatal("payload mismatch")
	}
}

func TestServerStreamNilEnc(t *testing.T) {
	var buf bytes.Buffer
	stream := NewServerStream(&buf, nil)
	if err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	_, err := stream.Write([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
}
