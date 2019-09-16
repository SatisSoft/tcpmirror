package util

import (
	"github.com/sirupsen/logrus"
	"io"
	"strconv"
	"strings"
)

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
