package rtmp

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	defaultChunkSize = 128
	defaultMaxMessageSize = 32 << 20
	defaultMaxChunkStreams = 1024
)

type messageHeader struct {
	Timestamp       uint32
	MessageLength   uint32
	MessageTypeID   uint8
	MessageStreamID uint32
}

type message struct {
	ChunkStreamID uint32
	Header        messageHeader
	Payload       []byte
}

func writeBasicHeader(w io.Writer, fmt uint8, csid uint32) error {
	b := byte((fmt&0x03)<<6) | byte(csid&0x3f)
	_, err := w.Write([]byte{b})
	return err
}

func readBasicHeader(r io.Reader) (fmt uint8, csid uint32, err error) {
	var b [1]byte
	_, err = io.ReadFull(r, b[:])
	if err != nil {
		return 0, 0, err
	}
	fmt = b[0] >> 6
	csid = uint32(b[0] & 0x3f)
	if csid == 0 {
		var b2 [1]byte
		_, err = io.ReadFull(r, b2[:])
		if err != nil {
			return 0, 0, err
		}
		return fmt, 64 + uint32(b2[0]), nil
	}
	if csid == 1 {
		var b2 [2]byte
		_, err = io.ReadFull(r, b2[:])
		if err != nil {
			return 0, 0, err
		}
		return fmt, 64 + uint32(b2[0]) + uint32(b2[1])*256, nil
	}
	return fmt, csid, nil
}

func writeMessageHeader(w io.Writer, fmt uint8, h messageHeader) error {
	if fmt != 0 {
		return io.ErrUnexpectedEOF
	}
	var b [11]byte
	ts := h.Timestamp
	if ts >= 0xFFFFFF {
		b[0], b[1], b[2] = 0xFF, 0xFF, 0xFF
	} else {
		b[0] = byte(ts >> 16)
		b[1] = byte(ts >> 8)
		b[2] = byte(ts)
	}
	ml := h.MessageLength
	b[3] = byte(ml >> 16)
	b[4] = byte(ml >> 8)
	b[5] = byte(ml)
	b[6] = h.MessageTypeID
	binary.LittleEndian.PutUint32(b[7:], h.MessageStreamID)
	_, err := w.Write(b[:])
	if err != nil {
		return err
	}
	if ts >= 0xFFFFFF {
		var ext [4]byte
		binary.BigEndian.PutUint32(ext[:], ts)
		_, err = w.Write(ext[:])
		if err != nil {
			return err
		}
	}
	return nil
}

func readMessageHeader(r io.Reader, fmt uint8, prev messageHeader) (h messageHeader, timestampOrDelta uint32, extTimestamp bool, isDelta bool, err error) {
	switch fmt {
	case 0:
		var b [11]byte
		_, err = io.ReadFull(r, b[:])
		if err != nil {
			return messageHeader{}, 0, false, false, err
		}
		ts := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
		if ts == 0xFFFFFF {
			extTimestamp = true
		}
		ml := uint32(b[3])<<16 | uint32(b[4])<<8 | uint32(b[5])
		h = messageHeader{
			Timestamp:       ts,
			MessageLength:   ml,
			MessageTypeID:   b[6],
			MessageStreamID: binary.LittleEndian.Uint32(b[7:]),
		}
		return h, ts, extTimestamp, false, nil
	case 1:
		var b [7]byte
		_, err = io.ReadFull(r, b[:])
		if err != nil {
			return messageHeader{}, 0, false, false, err
		}
		ts := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
		if ts == 0xFFFFFF {
			extTimestamp = true
		}
		ml := uint32(b[3])<<16 | uint32(b[4])<<8 | uint32(b[5])
		h = messageHeader{
			Timestamp:       ts,
			MessageLength:   ml,
			MessageTypeID:   b[6],
			MessageStreamID: prev.MessageStreamID,
		}
		return h, ts, extTimestamp, true, nil
	case 2:
		var b [3]byte
		_, err = io.ReadFull(r, b[:])
		if err != nil {
			return messageHeader{}, 0, false, false, err
		}
		ts := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
		if ts == 0xFFFFFF {
			extTimestamp = true
		}
		h = prev
		h.Timestamp = ts
		return h, ts, extTimestamp, true, nil
	case 3:
		return prev, 0, false, false, nil
	default:
		return messageHeader{}, 0, false, false, io.ErrUnexpectedEOF
	}
}

type chunkReader struct {
	r             io.Reader
	chunkSize     uint32
	prev          map[uint32]messageHeader
	lastDelta     map[uint32]uint32
	lastExt       map[uint32]bool
	lastDeltaMode map[uint32]bool
	inflight      map[uint32]*inflightMessage
	maxMessageSize  uint32
	maxChunkStreams int
}

type inflightMessage struct {
	header messageHeader
	buf    []byte
	read   uint32
}

func newChunkReader(r io.Reader) *chunkReader {
	return &chunkReader{
		r:             r,
		chunkSize:     defaultChunkSize,
		prev:          make(map[uint32]messageHeader),
		lastDelta:     make(map[uint32]uint32),
		lastExt:       make(map[uint32]bool),
		lastDeltaMode: make(map[uint32]bool),
		inflight:      make(map[uint32]*inflightMessage),
		maxMessageSize:  defaultMaxMessageSize,
		maxChunkStreams: defaultMaxChunkStreams,
	}
}

func (c *chunkReader) SetChunkSize(size uint32) {
	if size == 0 {
		return
	}
	c.chunkSize = size
}

func (c *chunkReader) ReadMessage() (*message, error) {
	for {
		fmt, csid, err := readBasicHeader(c.r)
		if err != nil {
			return nil, err
		}
		prevHeader, hasPrev := c.prev[csid]
		im := c.inflight[csid]
		if !hasPrev && im == nil && c.maxChunkStreams > 0 && len(c.prev) >= c.maxChunkStreams {
			return nil, errors.New("rtmp: too many chunk streams")
		}
		h, tsOrDelta, ext, isDelta, err := readMessageHeader(c.r, fmt, prevHeader)
		if err != nil {
			return nil, err
		}
		needExt := ext
		if fmt == 3 && c.lastExt[csid] {
			needExt = true
		}
		if needExt {
			var extb [4]byte
			_, err = io.ReadFull(c.r, extb[:])
			if err != nil {
				return nil, err
			}
			extVal := binary.BigEndian.Uint32(extb[:])
			if fmt == 0 {
				h.Timestamp = extVal
				tsOrDelta = extVal
				isDelta = false
			} else if fmt == 1 || fmt == 2 {
				tsOrDelta = extVal
				h.Timestamp = extVal
				isDelta = true
			} else if fmt == 3 {
				tsOrDelta = extVal
				isDelta = c.lastDeltaMode[csid]
				if !isDelta {
					h.Timestamp = extVal
				}
			}
		}

		newMessage := fmt == 0 || fmt == 1 || fmt == 2 || (fmt == 3 && im == nil)
		if fmt == 3 && im == nil && prevHeader.MessageLength == 0 && prevHeader.MessageTypeID == 0 && prevHeader.MessageStreamID == 0 && prevHeader.Timestamp == 0 {
			return nil, io.ErrUnexpectedEOF
		}

		if newMessage {
			if c.maxMessageSize > 0 && h.MessageLength > c.maxMessageSize {
				return nil, errors.New("rtmp: message too large")
			}
			if fmt == 0 {
				c.lastDelta[csid] = 0
				c.lastDeltaMode[csid] = false
			} else if fmt == 1 || fmt == 2 {
				c.lastDelta[csid] = tsOrDelta
				c.lastDeltaMode[csid] = true
			} else if fmt == 3 {
				c.lastDeltaMode[csid] = c.lastDeltaMode[csid]
			}
			if fmt == 0 {
				h.Timestamp = tsOrDelta
			} else if fmt == 1 || fmt == 2 {
				h.Timestamp = prevHeader.Timestamp + tsOrDelta
			} else if fmt == 3 {
				if isDelta {
					h.Timestamp = prevHeader.Timestamp + c.lastDelta[csid]
				} else {
					if needExt {
						h.Timestamp = tsOrDelta
					}
				}
			}
			c.prev[csid] = h
			c.lastExt[csid] = needExt
			im = &inflightMessage{header: h, buf: make([]byte, int(h.MessageLength))}
			c.inflight[csid] = im
		} else {
			if im == nil {
				return nil, io.ErrUnexpectedEOF
			}
			c.lastExt[csid] = needExt
		}

		remain := im.header.MessageLength - im.read
		toRead := c.chunkSize
		if remain < toRead {
			toRead = remain
		}
		_, err = io.ReadFull(c.r, im.buf[im.read:im.read+toRead])
		if err != nil {
			return nil, err
		}
		im.read += toRead
		if im.read == im.header.MessageLength {
			delete(c.inflight, csid)
			return &message{ChunkStreamID: csid, Header: im.header, Payload: im.buf}, nil
		}
	}
}

type chunkWriter struct {
	w         io.Writer
	chunkSize uint32
}

func newChunkWriter(w io.Writer) *chunkWriter {
	return &chunkWriter{w: w, chunkSize: defaultChunkSize}
}

func (c *chunkWriter) SetChunkSize(size uint32) {
	if size == 0 {
		return
	}
	c.chunkSize = size
}

func (c *chunkWriter) WriteMessage(csid uint32, h messageHeader, payload []byte) error {
	h.MessageLength = uint32(len(payload))
	remain := uint32(len(payload))
	offset := uint32(0)
	first := true
	for remain > 0 {
		chunkLen := c.chunkSize
		if remain < chunkLen {
			chunkLen = remain
		}
		if first {
			if err := writeBasicHeader(c.w, 0, csid); err != nil {
				return err
			}
			if err := writeMessageHeader(c.w, 0, h); err != nil {
				return err
			}
			first = false
		} else {
			if err := writeBasicHeader(c.w, 3, csid); err != nil {
				return err
			}
		}
		_, err := c.w.Write(payload[offset : offset+chunkLen])
		if err != nil {
			return err
		}
		offset += chunkLen
		remain -= chunkLen
	}
	return nil
}
