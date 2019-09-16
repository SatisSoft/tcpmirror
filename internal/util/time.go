package util

import "time"

const (
	Sec3Days      = 259200
	Millisec3Days = 259200000
)

func Milliseconds() int64 {
	return time.Now().UnixNano() / 1000000
}
