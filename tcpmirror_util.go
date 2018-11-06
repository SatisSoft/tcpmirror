package main

import (
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
)

func copyPack(packet []byte) []byte {
	packetCopy := make([]byte, len(packet))
	copy(packetCopy, packet)
	return packetCopy
}

func getMill() int64 {
	return time.Now().UnixNano() / 1000000
}

func printPacket(logger *logrus.Entry, s string, slice []byte) {
	sliceText := []string(nil)
	for i := range slice {
		number := slice[i]
		text := strconv.Itoa(int(number))
		sliceText = append(sliceText, text)
	}
	result := strings.Join(sliceText, ",")
	logger.Debugf("%s {%s}\n", s, result)
}
