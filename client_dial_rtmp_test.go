package xlive

import (
	"bytes"
	"crypto/cipher"
	"io"
	"net"
	"testing"

	xcrypto "github.com/shuffleman/xlive-tunnel/crypto"
	"github.com/shuffleman/xlive-tunnel/rtmp"
)

func TestNewClientDownRTMP(t *testing.T) {
	shared := make([]byte, 16)
	keyiv, err := xcrypto.DeriveKeyIV(shared)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := xcrypto.NewCFBEncrypter(keyiv)
	if err != nil {
		t.Fatal(err)
	}

	downLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer downLn.Close()

	go func() {
		conn, _ := downLn.Accept()
		ps := rtmp.NewPlayServer(conn, rtmp.PlayServerOptions{Enc: enc, StreamName: "live_test"})
		_ = ps.Start()
		_, _ = ps.Write([]byte("down"))
	}()

	uploadLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer uploadLn.Close()
	go func() {
		conn, _ := uploadLn.Accept()
		s, _ := rtmp.NewServer(conn, rtmp.ServerOptions{
			SelectDecryptor: func([]byte) (cipher.Stream, error) { return xcrypto.NewCFBDecrypter(keyiv) },
		})
		_, _ = s.Start()
		io.Copy(io.Discard, s)
	}()

	uploadConn, err := net.Dial("tcp", uploadLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer uploadConn.Close()
	downloadConn, err := net.Dial("tcp", downLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer downloadConn.Close()

	c, err := NewClient(ClientOptions{
		UploadConn:    uploadConn,
		DownloadConn:  downloadConn,
		DownloadProto: "rtmp",
		UUID:          "00000000-0000-0000-0000-000000000000",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	buf := make([]byte, 4)
	_, err = io.ReadFull(c, buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, []byte("down")) {
		t.Fatalf("downlink mismatch: got=%q", string(buf))
	}
}
