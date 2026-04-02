package flv

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
)

const (
	TagTypeAudio  = 8
	TagTypeVideo  = 9
	TagTypeScript = 18
)

func WriteHeader(w io.Writer) error {
	_, err := w.Write([]byte{'F', 'L', 'V', 0x01, 0x05, 0x00, 0x00, 0x00, 0x09})
	if err != nil {
		return err
	}
	var prev [4]byte
	_, err = w.Write(prev[:])
	return err
}

func WriteTag(w io.Writer, tagType byte, timestamp uint32, data []byte) error {
	var h [11]byte
	h[0] = tagType
	dataSize := uint32(len(data))
	h[1] = byte(dataSize >> 16)
	h[2] = byte(dataSize >> 8)
	h[3] = byte(dataSize)
	h[4] = byte(timestamp >> 16)
	h[5] = byte(timestamp >> 8)
	h[6] = byte(timestamp)
	h[7] = byte(timestamp >> 24)
	h[8], h[9], h[10] = 0, 0, 0
	_, err := w.Write(h[:])
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	if err != nil {
		return err
	}
	var prev [4]byte
	binary.BigEndian.PutUint32(prev[:], uint32(11+len(data)))
	_, err = w.Write(prev[:])
	return err
}

func ReadTag(r io.Reader) (tagType byte, timestamp uint32, data []byte, err error) {
	var h [11]byte
	_, err = io.ReadFull(r, h[:])
	if err != nil {
		return 0, 0, nil, err
	}
	tagType = h[0]
	dataSize := uint32(h[1])<<16 | uint32(h[2])<<8 | uint32(h[3])
	timestamp = uint32(h[4])<<16 | uint32(h[5])<<8 | uint32(h[6]) | uint32(h[7])<<24
	data = make([]byte, dataSize)
	_, err = io.ReadFull(r, data)
	if err != nil {
		return 0, 0, nil, err
	}
	var prev [4]byte
	_, err = io.ReadFull(r, prev[:])
	if err != nil {
		return 0, 0, nil, err
	}
	return tagType, timestamp, data, nil
}

func MetadataTag(duration float64, width float64, height float64, framerate float64) []byte {
	b := bytes.NewBuffer(nil)
	amf0WriteString(b, "onMetaData")
	amf0WriteECMAArray(b, map[string]any{
		"duration":     duration,
		"width":        width,
		"height":       height,
		"framerate":    framerate,
		"videocodecid": 7,
		"audiocodecid": 10,
	})
	return b.Bytes()
}

const (
	amf0Number    = 0x00
	amf0Boolean   = 0x01
	amf0String    = 0x02
	amf0Null      = 0x05
	amf0ECMAArray = 0x08
	amf0ObjectEnd = 0x09
)

func amf0WriteString(b *bytes.Buffer, s string) {
	b.WriteByte(amf0String)
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(s)))
	b.Write(l[:])
	b.WriteString(s)
}

func amf0WriteValue(b *bytes.Buffer, v any) {
	switch x := v.(type) {
	case string:
		amf0WriteString(b, x)
	case float64:
		b.WriteByte(amf0Number)
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], math.Float64bits(x))
		b.Write(n[:])
	case int:
		amf0WriteValue(b, float64(x))
	case bool:
		b.WriteByte(amf0Boolean)
		if x {
			b.WriteByte(1)
		} else {
			b.WriteByte(0)
		}
	case nil:
		b.WriteByte(amf0Null)
	default:
		b.WriteByte(amf0Null)
	}
}

func amf0WriteECMAArray(b *bytes.Buffer, obj map[string]any) {
	b.WriteByte(amf0ECMAArray)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(obj)))
	b.Write(l[:])
	for k, v := range obj {
		var kl [2]byte
		binary.BigEndian.PutUint16(kl[:], uint16(len(k)))
		b.Write(kl[:])
		b.WriteString(k)
		amf0WriteValue(b, v)
	}
	b.Write([]byte{0, 0, amf0ObjectEnd})
}
