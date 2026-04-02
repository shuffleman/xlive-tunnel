package xlive

import (
	"net"
	"testing"
)

func TestNewClientValidation(t *testing.T) {
	_, err := NewClient(ClientOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	_, err = NewClient(ClientOptions{UploadConn: c1, DownloadConn: c2})
	if err == nil {
		t.Fatal("expected error")
	}
	_, err = NewClient(ClientOptions{UploadConn: c1, DownloadConn: c2, UUID: "not-a-uuid"})
	if err == nil {
		t.Fatal("expected error")
	}
}
