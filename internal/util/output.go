package util

import (
	"io"
	"strconv"
	"strings"

	"github.com/egorban/navprot/pkg/egts"
	"github.com/egorban/navprot/pkg/ndtp"
	"github.com/sirupsen/logrus"
)

// PrintPacket prints packet in []byte{} format
func PrintPacket(logger *logrus.Entry, s string, slice []byte) {
	sliceText := []string(nil)
	for i := range slice {
		number := slice[i]
		text := strconv.Itoa(int(number))
		sliceText = append(sliceText, text)
	}
	result := strings.Join(sliceText, ",")
	logger.Debugf("%s {%s}", s, result)
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
	rest, err := packetData.Parse(slice)
	logger.Debugf("%s: %v; rest: %v, err: %v", s, packetData, rest, err)
}

// PrintEGTSPacketForDebugging prints packet for debugging purpose
func PrintEGTSPacketForDebugging(logger *logrus.Entry, s string, slice []byte) {
	packetData := new(egts.Packet)
	rest, err := packetData.Parse(slice)
	if err == nil {
		logger.Debugf("%s: %v; rest: %v, err: %v", s, packetData, rest, err)
	}
}

// Copy creates copy of binary packet
func Copy(packet []byte) []byte {
	packetCopy := make([]byte, len(packet))
	copy(packetCopy, packet)
	return packetCopy
}
