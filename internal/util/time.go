package util

import "time"

const (
	// Sec3Days is a number of seconds in 3 days
	Sec3Days = 259200
	// Millisec3Days is a number of milliseconds in 3 days
	Millisec3Days = 259200000
)

// Milliseconds returns current time in milliseconds
func Milliseconds() int64 {
	return time.Now().UnixNano() / 1000000
}
