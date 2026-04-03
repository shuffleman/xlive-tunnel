package xlive

import "runtime/debug"

func SetGCPercent(percent int) int {
	return debug.SetGCPercent(percent)
}

func SetMemoryLimitBytes(n int64) int64 {
	return debug.SetMemoryLimit(n)
}

func FreeOSMemory() {
	debug.FreeOSMemory()
}
