package httpflv

import (
	"bufio"
	"bytes"
	"crypto/cipher"
	"errors"
	"io"
	"net"
	"net/http/httputil"
	"strings"
	"sync"
	"time"

	"github.com/xlive-project/xlive/flv"
)

type ClientOptions struct {
	Path string
	Host string
	SID  string
	Dec  cipher.Stream
}

type ClientConn struct {
	raw net.Conn
	r   *bufio.Reader
	dec cipher.Stream

	once sync.Once

	tagBuf bytes.Buffer
}

var _ net.Conn = (*ClientConn)(nil)

func Dial(raw net.Conn, opts ClientOptions) (*ClientConn, error) {
	if opts.Path == "" {
		return nil, errors.New("httpflv: empty path")
	}
	if opts.Dec == nil {
		return nil, errors.New("httpflv: nil decryptor")
	}
	r := bufio.NewReaderSize(raw, 64*1024)
	path := opts.Path
	if strings.Contains(path, "{sid}") {
		path = strings.ReplaceAll(path, "{sid}", opts.SID)
	} else {
		if strings.Contains(path, "?") {
			path = path + "&token=" + opts.SID
		} else {
			path = path + "?token=" + opts.SID
		}
	}
	req := "GET " + path + " HTTP/1.1\r\nHost: " + opts.Host + "\r\nUser-Agent: Mozilla/5.0\r\nAccept: */*\r\nConnection: keep-alive\r\n\r\n"
	_, err := raw.Write([]byte(req))
	if err != nil {
		return nil, err
	}
	chunked, err := readHTTPHeaders(r)
	if err != nil {
		return nil, err
	}
	if chunked {
		r = bufio.NewReaderSize(httputil.NewChunkedReader(r), 64*1024)
	}
	err = readAndDiscardFLVHeader(r)
	if err != nil {
		return nil, err
	}
	_, _, _, _ = flv.ReadTag(r)
	return &ClientConn{raw: raw, r: r, dec: opts.Dec}, nil
}

func readHTTPHeaders(r *bufio.Reader) (chunked bool, err error) {
	_, err = r.ReadString('\n')
	if err != nil {
		return false, err
	}
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return false, err
		}
		if line == "\r\n" {
			return chunked, nil
		}
		lower := strings.ToLower(strings.TrimSpace(line))
		if strings.HasPrefix(lower, "transfer-encoding:") && strings.Contains(lower, "chunked") {
			chunked = true
		}
	}
}

func readAndDiscardFLVHeader(r *bufio.Reader) error {
	h := make([]byte, 9+4)
	_, err := io.ReadFull(r, h)
	return err
}

func (c *ClientConn) Read(p []byte) (n int, err error) {
	for c.tagBuf.Len() == 0 {
		tagType, _, data, err := flv.ReadTag(c.r)
		if err != nil {
			return 0, err
		}
		if tagType != flv.TagTypeVideo && tagType != flv.TagTypeAudio {
			continue
		}
		pt := make([]byte, len(data))
		c.dec.XORKeyStream(pt, data)
		c.tagBuf.Write(pt)
	}
	return c.tagBuf.Read(p)
}

func (c *ClientConn) Write(p []byte) (n int, err error) {
	return 0, io.ErrClosedPipe
}

func (c *ClientConn) Close() error {
	var err error
	c.once.Do(func() { err = c.raw.Close() })
	return err
}

func (c *ClientConn) LocalAddr() net.Addr  { return c.raw.LocalAddr() }
func (c *ClientConn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }
func (c *ClientConn) SetDeadline(t time.Time) error {
	return c.raw.SetDeadline(t)
}
func (c *ClientConn) SetReadDeadline(t time.Time) error {
	return c.raw.SetReadDeadline(t)
}
func (c *ClientConn) SetWriteDeadline(t time.Time) error {
	return c.raw.SetWriteDeadline(t)
}
