package rtmp

import (
	"bytes"
	"testing"
)

func TestExtractSessionIDFromConnect(t *testing.T) {
	sid := "abcdef0123456789abcdef0123456789"
	fp := DefaultFingerprint()
	p := buildConnectPayloadWithAppAndTcURL("live/"+sid, "rtmp://localhost/live/"+sid, fp)
	got, err := extractSessionIDFromConnect(p)
	if err != nil {
		t.Fatal(err)
	}
	if got != sid {
		t.Fatalf("got=%q want=%q", got, sid)
	}
}

func TestParseCommandNameAndTxID(t *testing.T) {
	p := buildCreateStreamPayload()
	name, tx, err := parseCommandNameAndTxID(p)
	if err != nil {
		t.Fatal(err)
	}
	if name != amfCmdCreateStream || tx != 2 {
		t.Fatalf("got name=%q tx=%v", name, tx)
	}
}

func TestChunkRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := newChunkWriter(&buf)
	w.SetChunkSize(4)
	h := messageHeader{Timestamp: 1, MessageTypeID: 20, MessageStreamID: 0}
	payload := []byte("0123456789")
	err := w.WriteMessage(3, h, payload)
	if err != nil {
		t.Fatal(err)
	}
	r := newChunkReader(&buf)
	r.SetChunkSize(4)
	m, err := r.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m.Payload, payload) {
		t.Fatal("payload mismatch")
	}
}

func TestChunkExtendedTimestamp(t *testing.T) {
	var buf bytes.Buffer
	w := newChunkWriter(&buf)
	w.SetChunkSize(1024)
	h := messageHeader{Timestamp: 0x1FFFFFE, MessageTypeID: 20, MessageStreamID: 0}
	payload := []byte("x")
	err := w.WriteMessage(3, h, payload)
	if err != nil {
		t.Fatal(err)
	}
	r := newChunkReader(&buf)
	r.SetChunkSize(1024)
	m, err := r.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m.Header.Timestamp != h.Timestamp {
		t.Fatalf("timestamp mismatch: got=%d want=%d", m.Header.Timestamp, h.Timestamp)
	}
}
