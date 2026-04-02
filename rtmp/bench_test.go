package rtmp

import (
	"crypto/cipher"
	"io"
	"net"
	"testing"

	xcrypto "github.com/shuffleman/xlive-tunnel/crypto"
)

func BenchmarkRTMPThroughput(b *testing.B) {
	shared := make([]byte, 16)
	keyiv, err := xcrypto.DeriveKeyIV(shared)
	if err != nil {
		b.Fatal(err)
	}
	enc, err := xcrypto.NewCFBEncrypter(keyiv)
	if err != nil {
		b.Fatal(err)
	}
	selectDec := func([]byte) (cipher.Stream, error) {
		return xcrypto.NewCFBDecrypter(keyiv)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()

	serverCh := make(chan *Server, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		s, err := NewServer(conn, ServerOptions{
			ChunkSize:       262144,
			SelectDecryptor: selectDec,
		})
		if err != nil {
			return
		}
		_, _ = s.Start()
		serverCh <- s
	}()

	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer c1.Close()

	client := NewClient(c1, ClientOptions{
		ChunkSize: 262144,
		Enc:       enc,
		SessionID: "0123456789abcdef0123456789abcdef",
	})
	err = client.Start()
	if err != nil {
		b.Fatal(err)
	}

	server := <-serverCh
	go io.Copy(io.Discard, server)

	block := make([]byte, 8<<20)
	b.SetBytes(int64(len(block)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = client.Write(block)
		if err != nil {
			b.Fatal(err)
		}
	}
}
