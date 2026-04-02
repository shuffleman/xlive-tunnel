package rtmp

import (
	"bytes"
	"testing"
)

func TestParseCommandInvalid(t *testing.T) {
	_, _, err := parseCommandNameAndTxID([]byte{0x02, 0x00})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClientWriteWithoutEnc(t *testing.T) {
	c := &Client{sid: "x"}
	_, err := c.Write([]byte("x"))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBasicHeaderInvalidCSID(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0)
	_, _, err := readBasicHeader(&buf)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestReadMessageHeaderUnsupportedFmt(t *testing.T) {
	_, _, _, _, err := readMessageHeader(bytes.NewReader(nil), 7, messageHeader{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWriteMessageHeaderUnsupportedFmt(t *testing.T) {
	var buf bytes.Buffer
	err := writeMessageHeader(&buf, 1, messageHeader{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestChunkReaderFmt3WithoutInflight(t *testing.T) {
	r := newChunkReader(bytes.NewReader([]byte{0xC2}))
	_, err := r.ReadMessage()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestExtractSessionIDMissingApp(t *testing.T) {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, "connect")
	amf0WriteNumber(b, 1)
	amf0WriteObject(b, map[string]amf0Value{})
	_, err := extractSessionIDFromConnect(b.Bytes())
	if err == nil {
		t.Fatal("expected error")
	}
}
