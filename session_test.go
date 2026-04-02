package xlive

import "testing"

func TestNewRandomSessionID(t *testing.T) {
	sid, err := NewRandomSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if len(sid) != 32 {
		t.Fatalf("unexpected length: %d", len(sid))
	}
}
