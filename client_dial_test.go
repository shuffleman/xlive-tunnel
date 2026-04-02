package xlive

import (
	"bufio"
	"bytes"
	"crypto/cipher"
	"io"
	"net"
	"strings"
	"testing"

	xcrypto "github.com/xlive-project/xlive/crypto"
	"github.com/xlive-project/xlive/httpflv"
	"github.com/xlive-project/xlive/rtmp"
)

func TestNewClientEndToEnd(t *testing.T) {
	shared := make([]byte, 16)
	keyiv, err := xcrypto.DeriveKeyIV(shared)
	if err != nil {
		t.Fatal(err)
	}

	uploadLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer uploadLn.Close()
	downloadLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer downloadLn.Close()

	uplinkGot := make(chan []byte, 1)
	go func() {
		conn, err := uploadLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		selectDec := func([]byte) (cipher.Stream, error) {
			return xcrypto.NewCFBDecrypter(keyiv)
		}
		s, err := rtmp.NewServer(conn, rtmp.ServerOptions{SelectDecryptor: selectDec})
		if err != nil {
			return
		}
		_, err = s.Start()
		if err != nil {
			return
		}
		buf := make([]byte, 6)
		_, _ = io.ReadFull(s, buf)
		uplinkGot <- buf
	}()

	downlinkWant := []byte("down")
	go func() {
		conn, err := downloadLn.Accept()
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
		enc, _ := xcrypto.NewCFBEncrypter(keyiv)
		stream := httpflv.NewServerStream(conn, enc)
		_ = stream.Start()
		_, _ = stream.Write(downlinkWant)
	}()

	uploadConn, err := net.Dial("tcp", uploadLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer uploadConn.Close()
	downloadConn, err := net.Dial("tcp", downloadLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer downloadConn.Close()

	c, err := NewClient(ClientOptions{
		UploadConn:   uploadConn,
		DownloadConn: downloadConn,
		DownloadHost: "example.com",
		DownloadPath: "/live/abc.flv",
		UUID:         "00000000-0000-0000-0000-000000000000",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Write([]byte("uplink"))
	if err != nil {
		t.Fatal(err)
	}
	gotUp := <-uplinkGot
	if !bytes.Equal(gotUp, []byte("uplink")) {
		t.Fatalf("uplink mismatch: got=%q", string(gotUp))
	}

	buf := make([]byte, len(downlinkWant))
	_, err = io.ReadFull(c, buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, downlinkWant) {
		t.Fatalf("downlink mismatch: got=%q want=%q", string(buf), string(downlinkWant))
	}

	if !strings.Contains(c.RemoteAddr().String(), ":") {
		t.Fatal("missing remote addr")
	}
}
