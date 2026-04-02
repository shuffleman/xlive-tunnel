package rtmp

// AAC packet type constants (second byte of Audio tag body)
const (
	aacPacketSeqHdr = 0x00 // AAC sequence header (AudioSpecificConfig)
	aacPacketRaw    = 0x01 // AAC raw frame data
)

// Audio tag first byte for AAC-LC 44100Hz 16bit stereo
// soundFormat(4b)=10(AAC) | soundRate(2b)=3(44kHz) | soundSize(1b)=1(16bit) | soundType(1b)=1(stereo)
// = 1010 | 11 | 1 | 1 = 0xAF
const aacSoundFormat = 0xAF

// AudioSpecificConfig for AAC-LC, 44100Hz, Stereo
// audioObjectType(5b)=2(AAC-LC) + samplingFrequencyIndex(4b)=4(44100) + channelConfiguration(4b)=2(stereo) + padding(3b)
// = 00010 | 0100 | 0010 | 000 → 0001 0010 0001 0000 = 0x12 0x10
var defaultAACConfig = []byte{0x12, 0x10}

// buildAACSeqHdrMessage builds the full Audio message body for the AAC sequence header.
// Format: [soundFormat 1B][aacPacketType 1B][AudioSpecificConfig]
func buildAACSeqHdrMessage() []byte {
	body := make([]byte, 2+len(defaultAACConfig))
	body[0] = aacSoundFormat  // AAC 44kHz 16bit stereo (0xAF)
	body[1] = aacPacketSeqHdr // sequence header
	copy(body[2:], defaultAACConfig)
	return body
}
