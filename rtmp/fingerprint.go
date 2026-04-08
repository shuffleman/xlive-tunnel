package rtmp

type Fingerprint struct {
	AppBase string

	TcURLScheme string
	TcURLHost   string

	SwfURL  string
	PageURL string

	FlashVer string
	Encoder  string

	ServerFmsVer   string
	ServerClientID string

	Meta Meta
}

type Meta struct {
	Width           float64
	Height          float64
	VideoDataRate   float64
	FrameRate       float64
	VideoCodecID    float64
	AudioDataRate   float64
	AudioSampleRate float64
	AudioSampleSize float64
	Stereo          bool
	AudioCodecID    float64
}

func DefaultFingerprint() Fingerprint {
	return Fingerprint{
		AppBase:        defaultRTMPAppBase,
		TcURLScheme:    defaultTcURLScheme,
		TcURLHost:      defaultTcURLHost,
		SwfURL:         "",
		PageURL:        "",
		FlashVer:       rtmpFlashVerFMLE,
		Encoder:        rtmpEncoderOBS,
		ServerFmsVer:   "FMS/3,0,1,123",
		ServerClientID: "NGINX RTMP (github.com/sergey-dryabzhinsky/nginx-rtmp-module)",
		Meta: Meta{
			Width:           1920.0,
			Height:          1080.0,
			VideoDataRate:   3000.0,
			FrameRate:       30.0,
			VideoCodecID:    7.0,
			AudioDataRate:   160.0,
			AudioSampleRate: 44100.0,
			AudioSampleSize: 16.0,
			Stereo:          true,
			AudioCodecID:    10.0,
		},
	}
}

func normalizeFingerprint(fp *Fingerprint) Fingerprint {
	if fp == nil {
		return DefaultFingerprint()
	}
	out := DefaultFingerprint()

	if fp.AppBase != "" {
		out.AppBase = fp.AppBase
	}
	if fp.TcURLScheme != "" {
		out.TcURLScheme = fp.TcURLScheme
	}
	if fp.TcURLHost != "" {
		out.TcURLHost = fp.TcURLHost
	}
	if fp.SwfURL != "" {
		out.SwfURL = fp.SwfURL
	}
	if fp.PageURL != "" {
		out.PageURL = fp.PageURL
	}
	if fp.FlashVer != "" {
		out.FlashVer = fp.FlashVer
	}
	if fp.Encoder != "" {
		out.Encoder = fp.Encoder
	}
	if fp.ServerFmsVer != "" {
		out.ServerFmsVer = fp.ServerFmsVer
	}
	if fp.ServerClientID != "" {
		out.ServerClientID = fp.ServerClientID
	}

	if fp.Meta.Width != 0 {
		out.Meta.Width = fp.Meta.Width
	}
	if fp.Meta.Height != 0 {
		out.Meta.Height = fp.Meta.Height
	}
	if fp.Meta.VideoDataRate != 0 {
		out.Meta.VideoDataRate = fp.Meta.VideoDataRate
	}
	if fp.Meta.FrameRate != 0 {
		out.Meta.FrameRate = fp.Meta.FrameRate
	}
	if fp.Meta.VideoCodecID != 0 {
		out.Meta.VideoCodecID = fp.Meta.VideoCodecID
	}
	if fp.Meta.AudioDataRate != 0 {
		out.Meta.AudioDataRate = fp.Meta.AudioDataRate
	}
	if fp.Meta.AudioSampleRate != 0 {
		out.Meta.AudioSampleRate = fp.Meta.AudioSampleRate
	}
	if fp.Meta.AudioSampleSize != 0 {
		out.Meta.AudioSampleSize = fp.Meta.AudioSampleSize
	}
	out.Meta.Stereo = fp.Meta.Stereo
	if fp.Meta.AudioCodecID != 0 {
		out.Meta.AudioCodecID = fp.Meta.AudioCodecID
	}

	return out
}
