package xlive

import (
	"crypto/rand"
	"encoding/hex"
)

func NewRandomSessionID() (string, error) {
	var b [16]byte
	_, err := rand.Read(b[:])
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
