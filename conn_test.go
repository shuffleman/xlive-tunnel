package xlive

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestClientConnReadWriteRouting(t *testing.T) {
	u1, u2 := net.Pipe()
	d1, d2 := net.Pipe()
	defer u1.Close()
	defer u2.Close()
	defer d1.Close()
	defer d2.Close()

	c := NewClientConn(u1, d1)
	go func() {
		_, _ = u2.Read(make([]byte, 5))
		_, _ = d2.Write([]byte("hello"))
	}()
	_, err := c.Write([]byte("aaaaa"))
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	_, err = io.ReadFull(c, buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != "hello" {
		t.Fatal("bad read")
	}
	_ = c.SetDeadline(time.Time{})
	_ = c.SetReadDeadline(time.Time{})
	_ = c.SetWriteDeadline(time.Time{})
	_ = c.LocalAddr()
	_ = c.RemoteAddr()
}

func TestServerConnReadWriteRouting(t *testing.T) {
	u1, u2 := net.Pipe()
	d1, d2 := net.Pipe()
	defer u1.Close()
	defer u2.Close()
	defer d1.Close()
	defer d2.Close()

	c := NewServerConn(u1, d1)
	go func() {
		_, _ = u2.Write([]byte("hello"))
		_, _ = d2.Read(make([]byte, 5))
	}()
	buf := make([]byte, 5)
	_, err := io.ReadFull(c, buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != "hello" {
		t.Fatal("bad read")
	}
	_, err = c.Write([]byte("aaaaa"))
	if err != nil {
		t.Fatal(err)
	}
}
