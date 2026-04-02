package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"io"

	"golang.org/x/crypto/hkdf"
)

const HKDFInfo = "xlive/encryption"

type KeyIV struct {
	Key [32]byte
	IV  [16]byte
}

func DeriveKeyIV(sharedSecret []byte) (KeyIV, error) {
	var out KeyIV
	r := hkdf.New(sha256.New, sharedSecret, nil, []byte(HKDFInfo))
	_, err := io.ReadFull(r, out.Key[:])
	if err != nil {
		return KeyIV{}, err
	}
	_, err = io.ReadFull(r, out.IV[:])
	if err != nil {
		return KeyIV{}, err
	}
	return out, nil
}

func NewCFBStreams(keyiv KeyIV) (enc cipher.Stream, dec cipher.Stream, err error) {
	block, err := aes.NewCipher(keyiv.Key[:])
	if err != nil {
		return nil, nil, err
	}
	ivEnc := keyiv.IV
	ivDec := keyiv.IV
	return cipher.NewCFBEncrypter(block, ivEnc[:]), cipher.NewCFBDecrypter(block, ivDec[:]), nil
}

func NewCFBEncrypter(keyiv KeyIV) (cipher.Stream, error) {
	block, err := aes.NewCipher(keyiv.Key[:])
	if err != nil {
		return nil, err
	}
	iv := keyiv.IV
	return cipher.NewCFBEncrypter(block, iv[:]), nil
}

func NewCFBDecrypter(keyiv KeyIV) (cipher.Stream, error) {
	block, err := aes.NewCipher(keyiv.Key[:])
	if err != nil {
		return nil, err
	}
	iv := keyiv.IV
	return cipher.NewCFBDecrypter(block, iv[:]), nil
}
