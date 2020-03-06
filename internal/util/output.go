package util

import (
	"github.com/ashirko/navprot/pkg/ndtp"
	"github.com/sirupsen/logrus"
	"io"
	"strconv"
	"strings"
)

// PrintPacket prints packet in []byte{} format
func PrintPacket(logger *logrus.Entry, s string, slice []byte) {
	sliceText := []string(nil)
	for i := range slice {
		number := slice[i]
		text := strconv.Itoa(int(number))
		sliceText = append(sliceText, text)
	}
	_ = strings.Join(sliceText, ",")
}

// CloseAndLog closes entity and log message if error occurs
func CloseAndLog(c io.Closer, logger *logrus.Entry) {
	err := c.Close()
	if err != nil {
		logger.Errorf("can't close %s:", err)
	}
}

// PrintPacketForDebugging prints packet for debugging purpose
func PrintPacketForDebugging(logger *logrus.Entry, s string, slice []byte) {
	packetData := new(ndtp.Packet)
	_, _ = packetData.Parse(slice)
}

// Copy creates copy of binary packet
func Copy(packet []byte) []byte {
	packetCopy := make([]byte, len(packet))
	copy(packetCopy, packet)
	return packetCopy
}
