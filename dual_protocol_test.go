package xlive

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"sync"
	"testing"

	xcrypto "github.com/shuffleman/xlive-tunnel/crypto"
	"github.com/shuffleman/xlive-tunnel/httpflv"
	"github.com/shuffleman/xlive-tunnel/rtmp"
)

func TestDualProtocolParallelOutput(t *testing.T) {
	shared := make([]byte, 16)
	keyiv, err := xcrypto.DeriveKeyIV(shared)
	if err != nil {
		t.Fatal(err)
	}

	rtmpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer rtmpLn.Close()

	httpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer httpLn.Close()

	go func() {
		conn, _ := rtmpLn.Accept()
		enc, _ := xcrypto.NewCFBEncrypter(keyiv)
		ps := rtmp.NewPlayServer(conn, rtmp.PlayServerOptions{Enc: enc, StreamName: "live_test"})
		_ = ps.Start()
		for i := 0; i < 32; i++ {
			_, _ = ps.Write([]byte("xxxxxxxxxxxxxxxx"))
		}
	}()
	go func() {
		conn, _ := httpLn.Accept()
		r := bufio.NewReader(conn)
		for {
			l, _ := r.ReadString('\n')
			if l == "\r\n" || l == "" {
				break
			}
		}
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: video/x-flv\r\n\r\n"))
		enc, _ := xcrypto.NewCFBEncrypter(keyiv)
		stream := httpflv.NewServerStream(conn, enc)
		_ = stream.Start()
		for i := 0; i < 32; i++ {
			_, _ = stream.Write([]byte("xxxxxxxxxxxxxxxx"))
		}
	}()

	rtmpConn, err := net.Dial("tcp", rtmpLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer rtmpConn.Close()
	httpConn, err := net.Dial("tcp", httpLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer httpConn.Close()

	decRTMP, err := xcrypto.NewCFBDecrypter(keyiv)
	if err != nil {
		t.Fatal(err)
	}
	pc, err := rtmp.DialPlay(rtmpConn, rtmp.PlayClientOptions{
		Dec:        decRTMP,
		SessionID:  "testsid",
		StreamName: "live_test",
	})
	if err != nil {
		t.Fatal(err)
	}
	decHTTP, err := xcrypto.NewCFBDecrypter(keyiv)
	if err != nil {
		t.Fatal(err)
	}
	hc, err := httpflv.Dial(httpConn, httpflv.ClientOptions{
		Path: "/live/test.flv",
		Host: "example.com",
		SID:  "testsid",
		Dec:  decHTTP,
	})
	if err != nil {
		t.Fatal(err)
	}

	total := 32 * len("xxxxxxxxxxxxxxxx")
	buf1 := make([]byte, total)
	buf2 := make([]byte, total)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.ReadFull(pc, buf1)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.ReadFull(hc, buf2)
	}()
	wg.Wait()
	if !bytes.Equal(buf1, buf2) {
		t.Fatal("mismatch outputs")
	}
}
