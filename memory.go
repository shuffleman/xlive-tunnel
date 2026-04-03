package xlive

import (
	"runtime"
	"runtime/debug"
	"sync"

	"github.com/shuffleman/xlive-tunnel/rtmp"
)

var DefaultIOSMemoryLimitBytes int64 = 40 << 20
var DefaultIOSGCPercent = 50
var DefaultIOSMaxMessageSize uint32 = 8 << 20
var DefaultIOSMaxChunkStreams = 256

var iosDefaultsOnce sync.Once

func applyIOSDefaults() {
	if runtime.GOOS != "ios" {
		return
	}
	iosDefaultsOnce.Do(func() {
		if DefaultIOSMemoryLimitBytes > 0 {
			_ = debug.SetMemoryLimit(DefaultIOSMemoryLimitBytes)
		}
		if DefaultIOSGCPercent > 0 {
			_ = debug.SetGCPercent(DefaultIOSGCPercent)
		}
		if DefaultIOSMaxMessageSize > 0 && rtmp.DefaultMaxMessageSize > DefaultIOSMaxMessageSize {
			rtmp.DefaultMaxMessageSize = DefaultIOSMaxMessageSize
		}
		if DefaultIOSMaxChunkStreams > 0 && rtmp.DefaultMaxChunkStreams > DefaultIOSMaxChunkStreams {
			rtmp.DefaultMaxChunkStreams = DefaultIOSMaxChunkStreams
		}
	})
}

func SetGCPercent(percent int) int {
	return debug.SetGCPercent(percent)
}

func SetMemoryLimitBytes(n int64) int64 {
	return debug.SetMemoryLimit(n)
}

func FreeOSMemory() {
	debug.FreeOSMemory()
}
