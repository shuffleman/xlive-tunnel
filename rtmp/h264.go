package rtmp

import "encoding/binary"

// H.264 AVC frame type constants (upper 4 bits of Video tag body byte 0)
const (
	avcFrameKeyframe = 0x17 // I-frame (1<<4) | AVC codec (7)
	avcFrameInter    = 0x27 // P-frame (2<<4) | AVC codec (7)
)

// AVC packet type (Video tag body byte 1)
const (
	avcPacketSeqHdr = 0x00 // AVC sequence header (SPS/PPS)
	avcPacketNALU   = 0x01 // AVC NALU data
)

// Pre-built SPS: H.264 High profile, Level 4.0, 1920x1080
// Commonly seen in real OBS x264 encoder output.
// NALU header byte 0x67 = forbidden_zero_bit=0, nal_ref_idc=3, nal_unit_type=7 (SPS)
var defaultSPS = []byte{
	0x67, 0x64, 0x00, 0x28, 0xAC, 0xD9, 0x40, 0x78,
	0x02, 0x27, 0xE5, 0xC0, 0x44, 0x00, 0x00, 0x03,
	0x00, 0x04, 0x00, 0x00, 0x03, 0x00, 0xC8, 0x3C,
	0x60, 0xC6, 0x58,
}

// Pre-built PPS
// NALU header byte 0x68 = forbidden_zero_bit=0, nal_ref_idc=3, nal_unit_type=8 (PPS)
var defaultPPS = []byte{0x68, 0xEE, 0x3C, 0x80}

// buildAVCDecoderConfig builds the AVCDecoderConfigurationRecord (ISO 14496-15).
// Real RTMP servers (nginx-rtmp, SRS) parse this record to extract SPS/PPS
// for codec negotiation; once accepted, subsequent NALU frames are forwarded as-is.
func buildAVCDecoderConfig() []byte {
	buf := make([]byte, 0, 11+len(defaultSPS)+len(defaultPPS))

	buf = append(buf,
		1,             // configurationVersion
		defaultSPS[1], // AVCProfileIndication (100 = High)
		defaultSPS[2], // profile_compatibility
		defaultSPS[3], // AVCLevelIndication (40 = Level 4.0)
		0xFF,          // reserved(6)=111111 + lengthSizeMinusOne(2)=11 → 4-byte NALU length
		0xE1,          // reserved(3)=111 + numOfSequenceParameterSets(5)=1
	)

	// SPS with 2-byte big-endian length prefix
	spsLen := make([]byte, 2)
	binary.BigEndian.PutUint16(spsLen, uint16(len(defaultSPS)))
	buf = append(buf, spsLen...)
	buf = append(buf, defaultSPS...)

	// PPS count
	buf = append(buf, 1)

	// PPS with 2-byte big-endian length prefix
	ppsLen := make([]byte, 2)
	binary.BigEndian.PutUint16(ppsLen, uint16(len(defaultPPS)))
	buf = append(buf, ppsLen...)
	buf = append(buf, defaultPPS...)

	return buf
}

// buildAVCSeqHdrMessage builds the full Video message body for the AVC sequence header.
// Format: [frameType|codecID 1B][avcPacketType 1B][compositionTime 3B][AVCDecoderConfigurationRecord]
func buildAVCSeqHdrMessage() []byte {
	config := buildAVCDecoderConfig()
	body := make([]byte, 5+len(config))
	body[0] = avcFrameKeyframe // keyframe + AVC
	body[1] = avcPacketSeqHdr  // sequence header
	// body[2:5] = compositionTime = 0x000000 (no B-frames in live streaming)
	copy(body[5:], config)
	return body
}
