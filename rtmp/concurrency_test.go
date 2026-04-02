package rtmp

import (
	"crypto/cipher"
	"io"
	"net"
	"sync"
	"testing"

	xcrypto "github.com/xlive-project/xlive/crypto"
)

func TestConcurrentWritesNoAckTimeout(t *testing.T) {
	shared := make([]byte, 16)
	keyiv, err := xcrypto.DeriveKeyIV(shared)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := xcrypto.NewCFBEncrypter(keyiv)
	if err != nil {
		t.Fatal(err)
	}
	selectDec := func([]byte) (cipher.Stream, error) {
		return xcrypto.NewCFBDecrypter(keyiv)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverCh := make(chan *Server, 1)
	go func() {
		conn, _ := ln.Accept()
		s, _ := NewServer(conn, ServerOptions{SelectDecryptor: selectDec})
		_, _ = s.Start()
		serverCh <- s
	}()

	cconn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(cconn, ClientOptions{
		Enc:        enc,
		SessionID:  "0123456789abcdef0123456789abcdef",
	})
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	server := <-serverCh
	go io.Copy(io.Discard, server)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 32; j++ {
				_, err := client.Write([]byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	_ = client.Close()
	_ = server.Close()
}

