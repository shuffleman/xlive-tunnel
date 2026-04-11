package httpflv

import (
	"crypto/cipher"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shuffleman/xlive-tunnel/flv"
)

type ServerStream struct {
	w   io.Writer
	enc cipher.Stream

	started bool
	seq     uint32

	mu sync.Mutex
}

func NewServerStream(w io.Writer, enc cipher.Stream) *ServerStream {
	return &ServerStream{w: w, enc: enc}
}

func (s *ServerStream) SetEnc(enc cipher.Stream) {
	s.mu.Lock()
	s.enc = enc
	s.mu.Unlock()
}

func (s *ServerStream) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	err := flv.WriteHeader(s.w)
	if err != nil {
		return err
	}
	meta := flv.MetadataTag(0, 1280, 720, 30)
	err = flv.WriteTag(s.w, flv.TagTypeScript, 0, meta)
	if err != nil {
		return err
	}
	s.started = true
	return nil
}

func (s *ServerStream) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		err = s.Start()
		if err != nil {
			return 0, err
		}
	}
	err = flv.WriteTag(s.w, flv.TagTypeVideo, s.seq, p)
	if err != nil {
		return 0, err
	}
	s.seq++
	return len(p), nil
}

type WriterConn struct {
	stream *ServerStream
	raw    io.Closer
	addr   net.Addr
	once   sync.Once
	closed atomic.Bool
}

var _ net.Conn = (*WriterConn)(nil)

func NewWriterConn(stream *ServerStream, closer io.Closer, addr net.Addr) *WriterConn {
	return &WriterConn{stream: stream, raw: closer, addr: addr}
}

func (c *WriterConn) Read(p []byte) (n int, err error) { return 0, io.EOF }
func (c *WriterConn) Write(p []byte) (n int, err error) {
	if c.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	return c.stream.Write(p)
}
func (c *WriterConn) Close() error {
	var err error
	c.once.Do(func() {
		c.closed.Store(true)
		if c.raw != nil {
			err = c.raw.Close()
		}
	})
	return err
}
func (c *WriterConn) LocalAddr() net.Addr                { return c.addr }
func (c *WriterConn) RemoteAddr() net.Addr               { return c.addr }
func (c *WriterConn) SetDeadline(t time.Time) error      { return nil }
func (c *WriterConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *WriterConn) SetWriteDeadline(t time.Time) error { return nil }
