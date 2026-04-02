package httpflv

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http/httputil"
	"strings"
	"testing"

	xcrypto "github.com/shuffleman/xlive-tunnel/crypto"
	"github.com/shuffleman/xlive-tunnel/flv"
)

func TestDialRequestPathFormatting(t *testing.T) {
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

	tests := []struct {
		name     string
		path     string
		wantPath string
		chunked  bool
	}{
		{name: "placeholder", path: "/live/abc.flv?token={sid}", wantPath: "/live/abc.flv?token=sid", chunked: false},
		{name: "append_query", path: "/live/abc.flv?x=1", wantPath: "/live/abc.flv?x=1&token=sid", chunked: false},
		{name: "append_path", path: "/live/abc.flv", wantPath: "/live/abc.flv?token=sid", chunked: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			gotReqLine := make(chan string, 1)
			done := make(chan struct{})
			go func() {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				br := bufio.NewReader(conn)
				line, _ := br.ReadString('\n')
				gotReqLine <- strings.TrimSpace(line)
				for {
					l, _ := br.ReadString('\n')
					if l == "\r\n" || l == "" {
						break
					}
				}
				if tt.chunked {
					_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: video/x-flv\r\nTransfer-Encoding: chunked\r\n\r\n"))
					cw := httputil.NewChunkedWriter(conn)
					stream := NewServerStream(cw, enc)
					_ = stream.Start()
					_, _ = stream.Write([]byte("x"))
					_ = cw.Close()
				} else {
					_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: video/x-flv\r\n\r\n"))
					stream := NewServerStream(conn, enc)
					_ = stream.Start()
					_, _ = stream.Write([]byte("x"))
				}
				<-done
			}()

			conn, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			cc, err := Dial(conn, ClientOptions{Path: tt.path, Host: "example.com", SID: "sid", Dec: dec})
			if err != nil {
				t.Fatal(err)
			}
			defer cc.Close()
			line := <-gotReqLine
			if !strings.HasPrefix(line, "GET "+tt.wantPath+" HTTP/1.1") {
				t.Fatalf("bad request line: %q", line)
			}
			b := make([]byte, 1)
			_, err = io.ReadFull(cc, b)
			if err != nil {
				t.Fatal(err)
			}
			close(done)
		})
	}
}

func TestReadSkipsNonMediaTags(t *testing.T) {
	shared := make([]byte, 16)
	keyiv, err := xcrypto.DeriveKeyIV(shared)
	if err != nil {
		t.Fatal(err)
	}
	enc, _ := xcrypto.NewCFBEncrypter(keyiv)
	dec, _ := xcrypto.NewCFBDecrypter(keyiv)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		for {
			l, _ := br.ReadString('\n')
			if l == "\r\n" || l == "" {
				break
			}
		}
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: video/x-flv\r\n\r\n"))
		stream := NewServerStream(conn, enc)
		_ = stream.Start()
		_ = flv.WriteTag(conn, flv.TagTypeScript, 0, []byte{1, 2, 3, 4})
		_, _ = stream.Write([]byte("ok"))
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	cc, err := Dial(conn, ClientOptions{Path: "/x", Host: "h", SID: "sid", Dec: dec})
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	var out bytes.Buffer
	_, err = io.CopyN(&out, cc, 2)
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "ok" {
		t.Fatalf("got=%q", out.String())
	}
}
