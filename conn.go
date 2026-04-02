package xlive

import (
	"net"
	"sync"
	"time"
)

type ClientConn struct {
	upload   net.Conn
	download net.Conn

	once sync.Once
}

type ServerConn struct {
	upload   net.Conn
	download net.Conn

	once sync.Once
}

var _ net.Conn = (*ClientConn)(nil)
var _ net.Conn = (*ServerConn)(nil)

func NewClientConn(upload net.Conn, download net.Conn) *ClientConn {
	return &ClientConn{
		upload:   upload,
		download: download,
	}
}

func NewServerConn(upload net.Conn, download net.Conn) *ServerConn {
	return &ServerConn{
		upload:   upload,
		download: download,
	}
}

func (c *ClientConn) Read(p []byte) (n int, err error) {
	return c.download.Read(p)
}

func (c *ClientConn) Write(p []byte) (n int, err error) {
	return c.upload.Write(p)
}

func (c *ClientConn) Close() error {
	c.once.Do(func() {
		_ = c.upload.Close()
		_ = c.download.Close()
	})
	return nil
}

func (c *ClientConn) LocalAddr() net.Addr {
	return c.upload.LocalAddr()
}

func (c *ClientConn) RemoteAddr() net.Addr {
	return c.upload.RemoteAddr()
}

func (c *ClientConn) SetDeadline(t time.Time) error {
	_ = c.upload.SetDeadline(t)
	_ = c.download.SetDeadline(t)
	return nil
}

func (c *ClientConn) SetReadDeadline(t time.Time) error {
	_ = c.download.SetReadDeadline(t)
	return nil
}

func (c *ClientConn) SetWriteDeadline(t time.Time) error {
	_ = c.upload.SetWriteDeadline(t)
	return nil
}

func (c *ServerConn) Read(p []byte) (n int, err error) {
	return c.upload.Read(p)
}

func (c *ServerConn) Write(p []byte) (n int, err error) {
	return c.download.Write(p)
}

func (c *ServerConn) Close() error {
	c.once.Do(func() {
		_ = c.upload.Close()
		_ = c.download.Close()
	})
	return nil
}

func (c *ServerConn) LocalAddr() net.Addr {
	return c.upload.LocalAddr()
}

func (c *ServerConn) RemoteAddr() net.Addr {
	return c.upload.RemoteAddr()
}

func (c *ServerConn) SetDeadline(t time.Time) error {
	_ = c.upload.SetDeadline(t)
	_ = c.download.SetDeadline(t)
	return nil
}

func (c *ServerConn) SetReadDeadline(t time.Time) error {
	_ = c.upload.SetReadDeadline(t)
	return nil
}

func (c *ServerConn) SetWriteDeadline(t time.Time) error {
	_ = c.download.SetWriteDeadline(t)
	return nil
}
