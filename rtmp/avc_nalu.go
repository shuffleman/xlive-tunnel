package rtmp

import "encoding/binary"

func parseAVCNALUs(videoPayload []byte) [][]byte {
	if len(videoPayload) < 5 {
		return nil
	}
	if videoPayload[1] != avcPacketNALU {
		return nil
	}
	p := videoPayload[5:]
	var out [][]byte
	for len(p) >= 4 {
		ln := int(binary.BigEndian.Uint32(p[:4]))
		p = p[4:]
		if ln <= 0 || ln > len(p) {
			return out
		}
		n := p[:ln]
		out = append(out, n)
		p = p[ln:]
	}
	return out
}

func buildFillerNALU(size int) []byte {
	if size <= 2 {
		return []byte{0x0c, 0x80}
	}
	n := make([]byte, 1+size)
	n[0] = 0x0c
	for i := 1; i < len(n)-1; i++ {
		n[i] = 0xff
	}
	n[len(n)-1] = 0x80
	return n
}
