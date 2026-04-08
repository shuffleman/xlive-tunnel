package rtmp

import (
	"bytes"
	"encoding/binary"
)

var seiUUID = [16]byte{0x78, 0x6c, 0x69, 0x76, 0x65, 0x2d, 0x74, 0x75, 0x6e, 0x6e, 0x65, 0x6c, 0x2d, 0x73, 0x65, 0x69}

func buildSEIUserDataUnregistered(ciphertext []byte) []byte {
	payload := make([]byte, 16+len(ciphertext))
	copy(payload[:16], seiUUID[:])
	copy(payload[16:], ciphertext)
	rbsp := buildSEIRBSPUserDataUnregistered(payload)
	nalu := make([]byte, 1+len(rbsp))
	nalu[0] = 0x06
	copy(nalu[1:], rbsp)
	return nalu
}

func buildSEIRBSPUserDataUnregistered(payload []byte) []byte {
	var b bytes.Buffer
	writeSEITypeAndSize(&b, 5, len(payload))
	epbWrite(&b, payload)
	b.WriteByte(0x80)
	return b.Bytes()
}

func writeSEITypeAndSize(b *bytes.Buffer, payloadType int, payloadSize int) {
	for payloadType >= 255 {
		b.WriteByte(0xFF)
		payloadType -= 255
	}
	b.WriteByte(byte(payloadType))
	for payloadSize >= 255 {
		b.WriteByte(0xFF)
		payloadSize -= 255
	}
	b.WriteByte(byte(payloadSize))
}

func extractSEIUserDataUnregistered(nalu []byte) (ciphertext []byte, ok bool) {
	if len(nalu) < 2 {
		return nil, false
	}
	if (nalu[0] & 0x1F) != 6 {
		return nil, false
	}
	rbsp := epbStrip(nalu[1:])
	i := 0
	for i < len(rbsp) {
		pt, ni := readSEIField(rbsp, i)
		if ni < 0 {
			return nil, false
		}
		i = ni
		ps, ni := readSEIField(rbsp, i)
		if ni < 0 {
			return nil, false
		}
		i = ni
		if i+ps > len(rbsp) {
			return nil, false
		}
		payload := rbsp[i : i+ps]
		i += ps
		if pt == 5 && len(payload) >= 16 && bytes.Equal(payload[:16], seiUUID[:]) {
			out := make([]byte, len(payload[16:]))
			copy(out, payload[16:])
			return out, true
		}
		for i < len(rbsp) && rbsp[i] == 0x00 {
			i++
		}
		if i < len(rbsp) && rbsp[i] == 0x80 {
			break
		}
	}
	return nil, false
}

func readSEIField(b []byte, off int) (val int, next int) {
	if off >= len(b) {
		return 0, -1
	}
	sum := 0
	for {
		if off >= len(b) {
			return 0, -1
		}
		x := int(b[off])
		off++
		sum += x
		if x != 255 {
			break
		}
	}
	return sum, off
}

func epbWrite(w *bytes.Buffer, payload []byte) {
	zeroCount := 0
	for i := 0; i < len(payload); i++ {
		x := payload[i]
		if zeroCount >= 2 && x <= 0x03 {
			w.WriteByte(0x03)
			zeroCount = 0
		}
		w.WriteByte(x)
		if x == 0x00 {
			zeroCount++
		} else {
			zeroCount = 0
		}
	}
}

func epbStrip(b []byte) []byte {
	out := make([]byte, 0, len(b))
	zeroCount := 0
	for i := 0; i < len(b); i++ {
		x := b[i]
		if zeroCount >= 2 && x == 0x03 {
			zeroCount = 0
			continue
		}
		out = append(out, x)
		if x == 0x00 {
			zeroCount++
		} else {
			zeroCount = 0
		}
	}
	return out
}

func buildAVCVideoBody(frameType byte, nalus ...[]byte) []byte {
	var payload bytes.Buffer
	for _, n := range nalus {
		if len(n) == 0 {
			continue
		}
		var ln [4]byte
		binary.BigEndian.PutUint32(ln[:], uint32(len(n)))
		payload.Write(ln[:])
		payload.Write(n)
	}
	body := make([]byte, 5+payload.Len())
	body[0] = frameType
	body[1] = avcPacketNALU
	copy(body[5:], payload.Bytes())
	return body
}
