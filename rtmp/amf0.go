package rtmp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

const (
	amf0Number    = 0x00
	amf0Boolean   = 0x01
	amf0String    = 0x02
	amf0Object    = 0x03
	amf0Null      = 0x05
	amf0ECMAArray = 0x08
	amf0ObjectEnd = 0x09
)

type amf0Value any

func amf0WriteString(b *bytes.Buffer, s string) {
	b.WriteByte(amf0String)
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(s)))
	b.Write(l[:])
	b.WriteString(s)
}

func amf0WriteNumber(b *bytes.Buffer, n float64) {
	b.WriteByte(amf0Number)
	var x [8]byte
	binary.BigEndian.PutUint64(x[:], math.Float64bits(n))
	b.Write(x[:])
}

func amf0WriteBool(b *bytes.Buffer, v bool) {
	b.WriteByte(amf0Boolean)
	if v {
		b.WriteByte(1)
	} else {
		b.WriteByte(0)
	}
}

func amf0WriteNull(b *bytes.Buffer) {
	b.WriteByte(amf0Null)
}

func amf0WriteObject(b *bytes.Buffer, obj map[string]amf0Value) {
	b.WriteByte(amf0Object)
	for k, v := range obj {
		var l [2]byte
		binary.BigEndian.PutUint16(l[:], uint16(len(k)))
		b.Write(l[:])
		b.WriteString(k)
		amf0WriteValue(b, v)
	}
	b.Write([]byte{0, 0, amf0ObjectEnd})
}

func amf0WriteECMAArray(b *bytes.Buffer, obj map[string]amf0Value) {
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

func amf0WriteValue(b *bytes.Buffer, v amf0Value) {
	switch x := v.(type) {
	case string:
		amf0WriteString(b, x)
	case float64:
		amf0WriteNumber(b, x)
	case int:
		amf0WriteNumber(b, float64(x))
	case uint32:
		amf0WriteNumber(b, float64(x))
	case bool:
		amf0WriteBool(b, x)
	case nil:
		amf0WriteNull(b)
	case map[string]amf0Value:
		amf0WriteObject(b, x)
	default:
		amf0WriteNull(b)
	}
}

type amf0Decoder struct {
	b []byte
	i int
}

func newAMF0Decoder(b []byte) *amf0Decoder { return &amf0Decoder{b: b} }

func (d *amf0Decoder) readByte() (byte, error) {
	if d.i >= len(d.b) {
		return 0, errors.New("amf0: eof")
	}
	x := d.b[d.i]
	d.i++
	return x, nil
}

func (d *amf0Decoder) readN(n int) ([]byte, error) {
	if d.i+n > len(d.b) {
		return nil, errors.New("amf0: eof")
	}
	x := d.b[d.i : d.i+n]
	d.i += n
	return x, nil
}

func (d *amf0Decoder) readStringRaw() (string, error) {
	lb, err := d.readN(2)
	if err != nil {
		return "", err
	}
	l := int(binary.BigEndian.Uint16(lb))
	sb, err := d.readN(l)
	if err != nil {
		return "", err
	}
	return string(sb), nil
}

func (d *amf0Decoder) readValue() (amf0Value, error) {
	t, err := d.readByte()
	if err != nil {
		return nil, err
	}
	switch t {
	case amf0Number:
		nb, err := d.readN(8)
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(binary.BigEndian.Uint64(nb)), nil
	case amf0Boolean:
		b, err := d.readByte()
		if err != nil {
			return nil, err
		}
		return b != 0, nil
	case amf0String:
		return d.readStringRaw()
	case amf0Null:
		return nil, nil
	case amf0Object:
		m := make(map[string]amf0Value)
		for {
			if d.i+3 <= len(d.b) && d.b[d.i] == 0 && d.b[d.i+1] == 0 && d.b[d.i+2] == amf0ObjectEnd {
				d.i += 3
				break
			}
			k, err := d.readStringRaw()
			if err != nil {
				return nil, err
			}
			v, err := d.readValue()
			if err != nil {
				return nil, err
			}
			m[k] = v
		}
		return m, nil
	case amf0ECMAArray:
		_, err := d.readN(4)
		if err != nil {
			return nil, err
		}
		m := make(map[string]amf0Value)
		for {
			if d.i+3 <= len(d.b) && d.b[d.i] == 0 && d.b[d.i+1] == 0 && d.b[d.i+2] == amf0ObjectEnd {
				d.i += 3
				break
			}
			k, err := d.readStringRaw()
			if err != nil {
				return nil, err
			}
			v, err := d.readValue()
			if err != nil {
				return nil, err
			}
			m[k] = v
		}
		return m, nil
	default:
		return nil, errors.New("amf0: unsupported type")
	}
}
