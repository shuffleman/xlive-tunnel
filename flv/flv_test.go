package flv

import (
	"bytes"
	"io"
	"testing"
)

func TestTagRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	err := WriteHeader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	meta := MetadataTag(0, 1920, 1080, 60)
	err = WriteTag(&buf, TagTypeScript, 0, meta)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("a"), 4096)
	err = WriteTag(&buf, TagTypeVideo, 12345, data)
	if err != nil {
		t.Fatal(err)
	}

	_, err = io.ReadAll(io.LimitReader(&buf, 13))
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = ReadTag(&buf)
	if err != nil {
		t.Fatal(err)
	}
	tagType, ts, got, err := ReadTag(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if tagType != TagTypeVideo || ts != 12345 {
		t.Fatalf("tag header mismatch: type=%d ts=%d", tagType, ts)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("tag data mismatch")
	}
}
