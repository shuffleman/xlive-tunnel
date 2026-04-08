package rtmp

const (
	defaultRTMPAppBase = "live"
	defaultTcURLHost   = "localhost"
	defaultTcURLScheme = "rtmp://"

	rtmpPublishTypeLive = "live"

	rtmpFlashVerFMLE = "FMLE/3.0 (compatible; FMSc/1.0)"
	rtmpEncoderOBS   = "OBS Server"

	amfCmdConnect       = "connect"
	amfCmdCreateStream  = "createStream"
	amfCmdReleaseStream = "releaseStream"
	amfCmdFCPublish     = "FCPublish"
	amfCmdPublish       = "publish"
	amfCmdPlay          = "play"
	amfCmdOnStatus      = "onStatus"
	amfCmdResult        = "_result"

	amfLevelStatus = "status"

	amfCodeNetConnectionConnectSuccess = "NetConnection.Connect.Success"
	amfDescConnectionSucceeded         = "Connection succeeded."

	amfCodeNetStreamPublishStart = "NetStream.Publish.Start"
	amfDescStartPublishing       = "Start publishing"

	amfCodeNetStreamPlayStart = "NetStream.Play.Start"
	amfDescStartPlaying       = "Start playing"
)
