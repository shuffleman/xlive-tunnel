package rtmp

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"time"
)

const (
	rtmpVersion   = 3
	handshakeSize = 1536
)

var (
	genuineFMSKey = []byte{
		0x47, 0x65, 0x6e, 0x75, 0x69, 0x6e, 0x65, 0x20,
		0x41, 0x64, 0x6f, 0x62, 0x65, 0x20, 0x46, 0x6c,
		0x61, 0x73, 0x68, 0x20, 0x4d, 0x65, 0x64, 0x69,
		0x61, 0x20, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72,
		0x20, 0x30, 0x30, 0x31,
		0xf0, 0xee, 0xc2, 0x4a, 0x80, 0x68, 0xbe, 0xe8,
		0x2e, 0x00, 0xd0, 0xd1, 0x02, 0x9e, 0x7e, 0x57,
		0x6e, 0xec, 0x5d, 0x2d, 0x29, 0x80, 0x6f, 0xab,
		0x93, 0xb8, 0xe6, 0x36, 0xcf, 0xeb, 0x31, 0xae,
	}
	genuineFPKey = []byte{
		0x47, 0x65, 0x6e, 0x75, 0x69, 0x6e, 0x65, 0x20,
		0x41, 0x64, 0x6f, 0x62, 0x65, 0x20, 0x46, 0x6c,
		0x61, 0x73, 0x68, 0x20, 0x50, 0x6c, 0x61, 0x79,
		0x65, 0x72, 0x20, 0x30, 0x30, 0x31,
		0xf0, 0xee, 0xc2, 0x4a, 0x80, 0x68, 0xbe, 0xe8,
		0x2e, 0x00, 0xd0, 0xd1, 0x02, 0x9e, 0x7e, 0x57,
		0x6e, 0xec, 0x5d, 0x2d, 0x29, 0x80, 0x6f, 0xab,
		0x93, 0xb8, 0xe6, 0x36, 0xcf, 0xeb, 0x31, 0xae,
	}
)

func writeC0C1(w io.Writer, now time.Time) ([]byte, error) {
	c1 := make([]byte, handshakeSize)
	binary.BigEndian.PutUint32(c1[0:4], uint32(now.Unix()))
	binary.BigEndian.PutUint32(c1[4:8], 0)
	_, err := rand.Read(c1[8:])
	if err != nil {
		return nil, err
	}
	_, err = w.Write(append([]byte{rtmpVersion}, c1...))
	if err != nil {
		return nil, err
	}
	return c1, nil
}

func readC0C1(r io.Reader) (c1 []byte, err error) {
	var v [1]byte
	_, err = io.ReadFull(r, v[:])
	if err != nil {
		return nil, err
	}
	if v[0] != rtmpVersion {
		return nil, io.ErrUnexpectedEOF
	}
	c1 = make([]byte, handshakeSize)
	_, err = io.ReadFull(r, c1)
	if err != nil {
		return nil, err
	}
	return c1, nil
}

func writeS0S1S2(w io.Writer, now time.Time, c1 []byte) ([]byte, error) {
	s1 := make([]byte, handshakeSize)
	binary.BigEndian.PutUint32(s1[0:4], uint32(now.Unix()))
	binary.BigEndian.PutUint32(s1[4:8], 0)
	_, err := rand.Read(s1[8:])
	if err != nil {
		return nil, err
	}

	s2 := make([]byte, handshakeSize)
	copy(s2, c1)
	binary.BigEndian.PutUint32(s2[0:4], uint32(now.Unix()))

	_, err = w.Write(append(append([]byte{rtmpVersion}, s1...), s2...))
	if err != nil {
		return nil, err
	}
	return s1, nil
}

func readS0S1S2(r io.Reader) (s1 []byte, err error) {
	var v [1]byte
	_, err = io.ReadFull(r, v[:])
	if err != nil {
		return nil, err
	}
	if v[0] != rtmpVersion {
		return nil, io.ErrUnexpectedEOF
	}
	s1 = make([]byte, handshakeSize)
	_, err = io.ReadFull(r, s1)
	if err != nil {
		return nil, err
	}
	s2 := make([]byte, handshakeSize)
	_, err = io.ReadFull(r, s2)
	if err != nil {
		return nil, err
	}
	return s1, nil
}

func writeC2(w io.Writer, s1 []byte) error {
	c2 := make([]byte, handshakeSize)
	copy(c2, s1)
	_, err := w.Write(c2)
	return err
}

func readC2(r io.Reader) error {
	var c2 [handshakeSize]byte
	_, err := io.ReadFull(r, c2[:])
	return err
}

func serverHandshake(rw io.ReadWriter) error {
	var c0 [1]byte
	_, err := io.ReadFull(rw, c0[:])
	if err != nil {
		return err
	}
	if c0[0] != rtmpVersion {
		return io.ErrUnexpectedEOF
	}
	c1 := make([]byte, handshakeSize)
	_, err = io.ReadFull(rw, c1)
	if err != nil {
		return err
	}
	scheme, c1DigestOff, c1Digest, ok := validateC1(c1)
	if !ok {
		_, err = writeS0S1S2(rw, time.Now(), c1)
		if err != nil {
			return err
		}
		return readC2(rw)
	}

	s1 := make([]byte, handshakeSize)
	binary.BigEndian.PutUint32(s1[0:4], uint32(time.Now().Unix()))
	binary.BigEndian.PutUint32(s1[4:8], 0x0D0E0A0D)
	_, err = rand.Read(s1[8:])
	if err != nil {
		return err
	}
	s1DigestOffset := calcDigestOffset(s1, scheme)
	writeDigest(s1, s1DigestOffset, computeDigest(s1, s1DigestOffset, genuineFMSKey[:36]))

	s2 := make([]byte, handshakeSize)
	_, err = rand.Read(s2[:])
	if err != nil {
		return err
	}
	tmpKey := hmacSHA256(c1Digest, genuineFMSKey)
	s2Digest := hmacSHA256(s2[:handshakeSize-32], tmpKey)
	copy(s2[handshakeSize-32:], s2Digest)

	_, err = rw.Write(append(append([]byte{rtmpVersion}, s1...), s2...))
	if err != nil {
		return err
	}
	_ = c1DigestOff
	return readC2(rw)
}

func validateC1(c1 []byte) (scheme int, digestOffset int, digest []byte, ok bool) {
	return validateDigest(c1, genuineFPKey[:30])
}

func validateS1(s1 []byte) (scheme int, digestOffset int, digest []byte, ok bool) {
	return validateDigest(s1, genuineFMSKey[:36])
}

func validateDigest(buf []byte, key []byte) (scheme int, digestOffset int, digest []byte, ok bool) {
	if len(buf) != handshakeSize {
		return 0, 0, nil, false
	}
	for _, sch := range []int{0, 1} {
		off := calcDigestOffset(buf, sch)
		if off < 0 || off+32 > len(buf) {
			continue
		}
		d := make([]byte, 32)
		copy(d, buf[off:off+32])
		expect := computeDigest(buf, off, key)
		if hmac.Equal(d, expect) {
			return sch, off, d, true
		}
	}
	return 0, 0, nil, false
}

func clientHandshake(rw io.ReadWriter) error {
	c1 := make([]byte, handshakeSize)
	binary.BigEndian.PutUint32(c1[0:4], uint32(time.Now().Unix()))
	binary.BigEndian.PutUint32(c1[4:8], 0)
	_, err := rand.Read(c1[8:])
	if err != nil {
		return err
	}
	c1DigestOffset := calcDigestOffset(c1, 1)
	writeDigest(c1, c1DigestOffset, computeDigest(c1, c1DigestOffset, genuineFPKey[:30]))

	_, err = rw.Write(append([]byte{rtmpVersion}, c1...))
	if err != nil {
		return err
	}

	var s0 [1]byte
	_, err = io.ReadFull(rw, s0[:])
	if err != nil {
		return err
	}
	if s0[0] != rtmpVersion {
		return io.ErrUnexpectedEOF
	}

	s1 := make([]byte, handshakeSize)
	_, err = io.ReadFull(rw, s1)
	if err != nil {
		return err
	}

	s2 := make([]byte, handshakeSize)
	_, err = io.ReadFull(rw, s2)
	if err != nil {
		return err
	}

	_, _, s1Digest, ok := validateS1(s1)

	c2 := make([]byte, handshakeSize)
	if ok {
		_, err = rand.Read(c2[:])
		if err != nil {
			return err
		}
		tmpKey := hmacSHA256(s1Digest, genuineFPKey)
		c2Digest := hmacSHA256(c2[:handshakeSize-32], tmpKey)
		copy(c2[handshakeSize-32:], c2Digest)
	} else {
		copy(c2, s1)
		binary.BigEndian.PutUint32(c2[0:4], uint32(time.Now().Unix()))
	}
	_, err = rw.Write(c2)
	return err
}

func calcDigestOffset(buf []byte, scheme int) int {
	if scheme == 0 {
		v := int(buf[8]) + int(buf[9]) + int(buf[10]) + int(buf[11])
		return (v%728 + 12)
	}
	v := int(buf[772]) + int(buf[773]) + int(buf[774]) + int(buf[775])
	return (v%728 + 776)
}

func computeDigest(buf []byte, off int, key []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(buf[:off])
	_, _ = h.Write(buf[off+32:])
	return h.Sum(nil)
}

func writeDigest(buf []byte, off int, digest []byte) {
	copy(buf[off:off+32], digest[:32])
}

func hmacSHA256(data []byte, key []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(data)
	return h.Sum(nil)
}
