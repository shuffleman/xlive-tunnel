package httpflv

import (
	"bufio"
	"crypto/cipher"
	"errors"
	"io"
	"net"
	"net/http/httputil"
	"strings"
	"sync"
	"time"

	"github.com/shuffleman/xlive-tunnel/flv"
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

	inTag     bool
	tagType   byte
	tagRemain uint32
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
	_ = discardOneTag(r)
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
	if len(p) == 0 {
		return 0, nil
	}

	for {
		if c.inTag {
			if c.tagRemain == 0 {
				if err := discardN(c.r, 4); err != nil {
					return 0, err
				}
				c.inTag = false
				continue
			}
			toRead := uint32(len(p))
			if c.tagRemain < toRead {
				toRead = c.tagRemain
			}
			n, err := c.r.Read(p[:toRead])
			if n > 0 {
				c.dec.XORKeyStream(p[:n], p[:n])
				c.tagRemain -= uint32(n)
				return n, nil
			}
			return 0, err
		}

		tagType, dataSize, err := readTagHeader(c.r)
		if err != nil {
			return 0, err
		}

		if tagType != flv.TagTypeVideo && tagType != flv.TagTypeAudio {
			if err := discardN(c.r, int64(dataSize)+4); err != nil {
				return 0, err
			}
			continue
		}

		c.inTag = true
		c.tagType = tagType
		c.tagRemain = dataSize
	}
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

func readTagHeader(r io.Reader) (tagType byte, dataSize uint32, err error) {
	var h [11]byte
	_, err = io.ReadFull(r, h[:])
	if err != nil {
		return 0, 0, err
	}
	tagType = h[0]
	dataSize = uint32(h[1])<<16 | uint32(h[2])<<8 | uint32(h[3])
	return tagType, dataSize, nil
}

func discardOneTag(r io.Reader) error {
	_, dataSize, err := readTagHeader(r)
	if err != nil {
		return err
	}
	return discardN(r, int64(dataSize)+4)
}

func discardN(r io.Reader, n int64) error {
	_, err := io.CopyN(io.Discard, r, n)
	return err
}
