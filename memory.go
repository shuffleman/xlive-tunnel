package xlive

import (
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/shuffleman/xlive-tunnel/rtmp"
)

var DefaultIOSMemoryLimitBytes int64 = 30 << 20
var DefaultIOSGCPercent = 20
var DefaultIOSMaxMessageSize uint32 = 4 << 20
var DefaultIOSMaxChunkStreams = 128
var DefaultIOSMaxFramePayload = 32 * 1024
var DefaultIOSFreeOSMemoryInterval = time.Second

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
		if DefaultIOSMaxFramePayload > 0 && rtmp.DefaultMaxFramePayload > DefaultIOSMaxFramePayload {
			rtmp.DefaultMaxFramePayload = DefaultIOSMaxFramePayload
		}
		if DefaultIOSFreeOSMemoryInterval > 0 {
			go func() {
				t := time.NewTicker(DefaultIOSFreeOSMemoryInterval)
				defer t.Stop()
				for range t.C {
					debug.FreeOSMemory()
				}
			}()
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
