package httpflv

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"
)

func TestReadHTTPHeadersChunked(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"))
	chunked, err := readHTTPHeaders(r)
	if err != nil {
		t.Fatal(err)
	}
	if !chunked {
		t.Fatal("expected chunked")
	}
}

type panicConn struct{}

func (panicConn) Read([]byte) (int, error)         { panic("unexpected") }
func (panicConn) Write([]byte) (int, error)        { panic("unexpected") }
func (panicConn) Close() error                     { panic("unexpected") }
func (panicConn) LocalAddr() net.Addr              { return nil }
func (panicConn) RemoteAddr() net.Addr             { return nil }
func (panicConn) SetDeadline(time.Time) error      { panic("unexpected") }
func (panicConn) SetReadDeadline(time.Time) error  { panic("unexpected") }
func (panicConn) SetWriteDeadline(time.Time) error { panic("unexpected") }

func TestDialValidation(t *testing.T) {
	_, err := Dial(panicConn{}, ClientOptions{Path: "", Host: "h", SID: "s", Dec: nil})
	if err == nil {
		t.Fatal("expected error")
	}
}
