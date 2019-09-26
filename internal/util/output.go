package util

import (
	"github.com/ashirko/navprot/pkg/ndtp"
	"github.com/sirupsen/logrus"
	"io"
	"strconv"
	"strings"
)

// Print packet in []byte{} format
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

func CloseAndLog(c io.Closer, logger *logrus.Entry) {
	err := c.Close()
	if err != nil {
		logger.Errorf("can't close %s:", err)
	}
}

// for test only
func PrintPacketForDebugging(logger *logrus.Entry, s string, slice []byte) {
	packetData := new(ndtp.Packet)
	rest, err := packetData.Parse(slice)
	logger.Debugf("%s: %v; rest: %v, err: %v", s, packetData, rest, err)
}

func Copy(packet []byte) []byte {
	packetCopy := make([]byte, len(packet))
	copy(packetCopy, packet)
	return packetCopy
}
