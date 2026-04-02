package rtmp

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestChunkReaderFmt1AndFmt2(t *testing.T) {
	var buf bytes.Buffer

	write := func(b []byte) {
		_, _ = buf.Write(b)
	}

	write([]byte{0x03})
	write([]byte{
		0x00, 0x00, 0x00,
		0x00, 0x00, 0x04,
		messageTypeSetChunkSize,
		0x00, 0x00, 0x00, 0x00,
	})
	var chunkSize [4]byte
	binary.BigEndian.PutUint32(chunkSize[:], 4096)
	write(chunkSize[:])

	write([]byte{0x43})
	write([]byte{
		0x00, 0x00, 0x05,
		0x00, 0x00, 0x04,
		messageTypeSetChunkSize,
	})
	binary.BigEndian.PutUint32(chunkSize[:], 8192)
	write(chunkSize[:])

	write([]byte{0x83})
	write([]byte{0x00, 0x00, 0x07})
	binary.BigEndian.PutUint32(chunkSize[:], 16384)
	write(chunkSize[:])

	r := newChunkReader(&buf)
	m1, err := r.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m1.Header.Timestamp != 0 {
		t.Fatal("unexpected timestamp")
	}

	m2, err := r.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m2.Header.Timestamp != 5 {
		t.Fatalf("unexpected timestamp: %d", m2.Header.Timestamp)
	}

	m3, err := r.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m3.Header.Timestamp != 12 {
		t.Fatalf("unexpected timestamp: %d", m3.Header.Timestamp)
	}
}

