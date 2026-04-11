package crypto

import "crypto/cipher"

type identityStream struct{}

func (identityStream) XORKeyStream(dst, src []byte) {
	copy(dst, src)
}

func IdentityStream() cipher.Stream {
	return identityStream{}
}
