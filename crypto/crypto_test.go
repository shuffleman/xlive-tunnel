package crypto

import (
	"bytes"
	"testing"
)

func TestCFBRoundTrip(t *testing.T) {
	shared := []byte("0123456789abcdef")
	keyiv, err := DeriveKeyIV(shared)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := NewCFBEncrypter(keyiv)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := NewCFBDecrypter(keyiv)
	if err != nil {
		t.Fatal(err)
	}
	plain := bytes.Repeat([]byte{0x11, 0x22, 0x33, 0x44}, 1024)
	ct := make([]byte, len(plain))
	enc.XORKeyStream(ct, plain)
	out := make([]byte, len(plain))
	dec.XORKeyStream(out, ct)
	if !bytes.Equal(out, plain) {
		t.Fatal("decrypt mismatch")
	}
}

func TestNewCFBStreams(t *testing.T) {
	shared := []byte("0123456789abcdef")
	keyiv, err := DeriveKeyIV(shared)
	if err != nil {
		t.Fatal(err)
	}
	enc, dec, err := NewCFBStreams(keyiv)
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("xlive")
	ct := make([]byte, len(plain))
	enc.XORKeyStream(ct, plain)
	out := make([]byte, len(plain))
	dec.XORKeyStream(out, ct)
	if !bytes.Equal(out, plain) {
		t.Fatal("decrypt mismatch")
	}
}
