package main

import (
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

func copyPack(packet []byte) []byte {
	packetCopy := make([]byte, len(packet))
	copy(packetCopy, packet)
	return packetCopy
}

func milliseconds() int64 {
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

func ip(c net.Conn) net.IP {
	ipPort := strings.Split(c.RemoteAddr().String(), ":")
	ip := ipPort[0]
	ip1 := net.ParseIP(ip)
	return ip1.To4()
}

func closeAndLog(c io.Closer, logger *logrus.Entry) {
	err := c.Close()
	if err != nil {
		logger.Errorf("can't close %s:", err)
	}
}
