package main

import "time"

func copyPack(packet []byte) []byte {
	packetCopy := make([]byte, len(packet))
	copy(packetCopy, packet)
	return packetCopy
}

func getMill() int64 {
	return time.Now().UnixNano() / 1000000
}
