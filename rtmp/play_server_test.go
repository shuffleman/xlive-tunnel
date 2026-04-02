package rtmp

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	xcrypto "github.com/shuffleman/xlive-tunnel/crypto"
)

func TestPlayServerBasic(t *testing.T) {
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

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverReady := make(chan struct{})
	go func() {
		conn, _ := ln.Accept()
		ps := NewPlayServer(conn, PlayServerOptions{Enc: enc, StreamName: "live_test"})
		_ = ps.Start()
		close(serverReady)
		_, _ = ps.Write([]byte("hello"))
		_, _ = ps.Write([]byte("world"))
		time.Sleep(50 * time.Millisecond)
		_ = ps.Close()
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := clientHandshake(c); err != nil {
		t.Fatal(err)
	}
	cc := newConn(c)
	cc.cr.SetChunkSize(262144)

	if err := cc.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, buildConnectPayload("test")); err != nil {
		t.Fatal(err)
	}
loop:
	for {
		msg, err := cc.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if msg.Header.MessageTypeID == messageTypeCommandAMF0 {
			break loop
		}
	}
	if err := cc.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 0,
	}, buildCreateStreamPayload()); err != nil {
		t.Fatal(err)
	}
loop2:
	for {
		msg, err := cc.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if msg.Header.MessageTypeID == messageTypeCommandAMF0 {
			break loop2
		}
	}
	buf := bytes.NewBuffer(nil)
	amf0WriteString(buf, "play")
	amf0WriteNumber(buf, 3)
	amf0WriteNull(buf)
	amf0WriteString(buf, "live_test")
	if err := cc.writeRawMessage(csidCommand, messageHeader{
		Timestamp:       0,
		MessageTypeID:   messageTypeCommandAMF0,
		MessageStreamID: 1,
	}, buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	<-serverReady

	var got []byte
	for len(got) < len("helloworld") {
		m, err := cc.ReadMessage()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		switch m.Header.MessageTypeID {
		case messageTypeAudio:
			if len(m.Payload) >= 2 {
				if m.Payload[1] == aacPacketRaw {
					data := append([]byte(nil), m.Payload[2:]...)
					dec.XORKeyStream(data, data)
					got = append(got, data...)
				}
			}
		case messageTypeVideo:
			if len(m.Payload) >= 9 {
				if m.Payload[1] == avcPacketNALU {
					n := int(binary.BigEndian.Uint32(m.Payload[5:9]))
					if 9+n <= len(m.Payload) {
						data := append([]byte(nil), m.Payload[9:9+n]...)
						dec.XORKeyStream(data, data)
						got = append(got, data...)
					}
				}
			}
		default:
		}
	}
	if !bytes.Equal(got, []byte("helloworld")) {
		t.Fatalf("mismatch: got=%q", string(got))
	}
}
