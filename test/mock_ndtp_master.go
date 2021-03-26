package test

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
)

func mockNdtpMaster(t *testing.T, addr string) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_master"})
	logger.Tracef("start mock_master")
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer util.CloseAndLog(l, logger, time.Now().UnixNano(), "test")
	for {
		conn, err := l.Accept()
		if err != nil {
			logger.Errorf("error while accepting: %s", err)
		}
		defer util.CloseAndLog(conn, logger, time.Now().UnixNano(), "test")
		logrus.Printf("accepted connection (%s <-> %s)", conn.RemoteAddr(), conn.LocalAddr())
		go startMockNdtpMaster(t, conn, logger)
	}
}

func mockNdtpMasterWithControl(t *testing.T, addr string) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_master"})
	logger.Tracef("start mock_master")
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer util.CloseAndLog(l, logger, time.Now().UnixNano(), "test")
	for {
		conn, err := l.Accept()
		if err != nil {
			logger.Errorf("error while accepting: %s", err)
		}
		defer util.CloseAndLog(conn, logger, time.Now().UnixNano(), "test")
		logrus.Printf("accepted connection (%s <-> %s)", conn.RemoteAddr(), conn.LocalAddr())
		go startWithControl(t, conn, logger)
	}
}

func startMockNdtpMaster(t *testing.T, conn net.Conn, logger *logrus.Entry) {
	for {
		err := receiveAndReply(t, conn, logger)
		if err != nil {
			logger.Tracef("got error: %v", err)
			return
		}
	}
}

func startWithControl(t *testing.T, conn net.Conn, logger *logrus.Entry) {
	err := sendAndReceiveControl(t, conn, logger)
	if err != nil {
		logger.Tracef("got error: %v", err)
		return
	}
}

func sendAndReceiveControl(t *testing.T, c net.Conn, logger *logrus.Entry) (err error) {
	err = receiveAndReply(t, c, logger)
	if err != nil {
		logger.Tracef("got error: %v", err)
		return
	}
	err = send(c, packetControl)
	logger.Tracef("send: %v", packetControl)
	if err != nil {
		return
	}
	var b [defaultBufferSize]byte
	n, err := c.Read(b[:])
	logger.Tracef("receive: %v", b[:n])
	if !bytes.Equal(b[:n], packetControlReply) {
		logger.Tracef("expected: %v", packetControlReply)
		logger.Tracef("got: %v", b[:n])
		t.Errorf("expected control packet reply but received %s", b[:n])
	}
	return
}
