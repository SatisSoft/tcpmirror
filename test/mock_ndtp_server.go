package test

import (
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
	"net"
	"testing"
)

func mockNdtpServer(t *testing.T, addr string) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_ndtp_client"})
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer util.CloseAndLog(l, logger)
	for {
		conn, err := l.Accept()
		if err != nil {
			logger.Errorf("error while accepting: %s", err)
		}
		defer util.CloseAndLog(conn, logger)
		logrus.Printf("accepted connection (%s <-> %s)", conn.RemoteAddr(), conn.LocalAddr())
		go startMockNdtpClient(t, conn, logger)
	}
}

func startMockNdtpClient(t *testing.T, conn net.Conn, logger *logrus.Entry) {
	for {
		err := receiveAndReply(t, conn, logger)
		if err != nil {
			logger.Tracef("got error: %v", err)
			return
		}
	}
}
