package test

import (
	"github.com/ashirko/navprot/pkg/ndtp"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
	"net"
	"testing"
	"time"
)

var packetAuth = []byte{1, 2, 3, 126, 126, 59, 0, 2, 0, 14, 84, 2, 0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 1, 0, 0, 0, 0, 0, 6, 0, 2, 0, 2, 3, 90, 139, 1, 0, 0, 4, 0, 0, 0, 0, 0, 0, 51, 53, 53, 48, 57, 52, 48, 52, 51, 49, 56, 56, 51, 49, 49, 50, 53, 48, 48, 49, 54, 53, 48, 53, 56, 49, 53, 53, 51, 55, 0, 1, 2, 3}
var packetAuthSecond = []byte{1, 2, 3, 126, 126, 59, 0, 2, 0, 222, 186, 2, 0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 1, 0, 0, 0, 0, 0, 6, 0, 2, 0, 2, 3, 90, 128, 1, 0, 0, 4, 0, 0, 0, 0, 0, 0, 51, 53, 53, 48, 57, 52, 48, 52, 51, 49, 56, 56, 51, 49, 49, 50, 53, 48, 48, 49, 54, 53, 48, 53, 56, 49, 53, 53, 51, 55, 0}

var packetNav = []byte{126, 126, 74, 0, 2, 0, 107, 210, 2, 0, 0, 0, 0, 0, 0, 1, 0, 101, 0, 1, 0, 171,
	20, 0, 0, 0, 0, 36, 141, 198, 90, 87, 110, 119, 22, 201, 186, 64, 33, 224, 203, 0, 0, 0, 0, 83, 1, 0,
	0, 220, 0, 4, 0, 2, 0, 22, 0, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 167, 97, 0, 0, 31, 6, 0, 0, 8,
	0, 2, 0, 0, 0, 0, 0}

var packetExt = []byte{126, 126, 90, 1, 2, 0, 33, 134, 2, 0, 4, 0, 0, 144, 7, 5, 0, 100, 0, 0, 0, 1, 0, 0, 0, 18, 0, 0, 128, 0, 0, 0, 0, 1, 0, 0, 0, 60, 78, 65, 86, 83, 67, 82, 32, 118, 101, 114, 61, 49, 46, 48, 62, 60, 73, 68, 62, 49, 56, 60, 47, 73, 68, 62, 60, 70, 82, 79, 77, 62, 83, 69, 82, 86, 69, 82, 60, 47, 70, 82, 79, 77, 62, 60, 84, 79, 62, 85, 83, 69, 82, 60, 47, 84, 79, 62, 60, 84, 89, 80, 69, 62, 81, 85, 69, 82, 89, 60, 47, 84, 89, 80, 69, 62, 60, 77, 83, 71, 32, 116, 105, 109, 101, 61, 54, 48, 32, 98, 101, 101, 112, 61, 49, 32, 116, 121, 112, 101, 61, 98, 97, 99, 107, 103, 114, 111, 117, 110, 100, 62, 60, 98, 114, 47, 62, 60, 98, 114, 47, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 194, 251, 32, 236, 229, 237, 255, 32, 241, 235, 251, 248, 232, 242, 229, 63, 60, 98, 114, 47, 62, 60, 98, 114, 47, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 60, 98, 116, 110, 49, 62, 196, 224, 60, 47, 98, 116, 110, 49, 62, 60, 98, 114, 47, 62, 60, 98, 114, 47, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 60, 98, 116, 110, 50, 62, 205, 229, 242, 60, 47, 98, 116, 110, 50, 62, 60, 98, 114, 47, 62, 60, 47, 77, 83, 71, 62, 60, 47, 78, 65, 86, 83, 67, 82, 62}

func mockTerminal(t *testing.T, addr string, num int) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_terminal"})
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer util.CloseAndLog(conn, logger)
	err = sendAndReceive(t, conn, packetAuth, logger)
	if err != nil {
		logger.Errorf("got error: %v", err)
		t.Error(err)
	}
	for i := 0; i < num; i++ {
		err = sendNewMessage(t, conn, i, logger)
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
	time.Sleep(10 * time.Second)
}

func mockTerminalSecond(t *testing.T, addr string, num int) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_terminal"})
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer util.CloseAndLog(conn, logger)
	err = sendAndReceive(t, conn, packetAuthSecond, logger)
	if err != nil {
		logger.Errorf("got error: %v", err)
		t.Error(err)
	}
	//time.Sleep(100 * time.Millisecond)
	for i := 0; i < num; i++ {
		err = sendNewMessage(t, conn, i, logger)
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
	time.Sleep(1 * time.Second)
}

func mockTerminalGuaranteedDeliveryMaster(t *testing.T, addr string, num int, sleep int) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_terminal"})
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer util.CloseAndLog(conn, logger)
	err = sendAndReceive(t, conn, packetAuthSecond, logger)
	if err != nil {
		logger.Errorf("got error: %v", err)
		t.Error(err)
	}
	for i := 0; i < num; i++ {
		err = sendNewMessage(t, conn, i, logger)
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
	time.Sleep(time.Duration(sleep) * time.Second)
}

func mockTerminalGuaranteedDeliveryTwoServers(t *testing.T, addr string, num int) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_terminal"})
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer util.CloseAndLog(conn, logger)
	err = sendAndReceive(t, conn, packetAuthSecond, logger)
	if err != nil {
		logger.Errorf("got error: %v", err)
		t.Error(err)
	}
	for i := 0; i < num; i++ {
		err = sendNewMessage(t, conn, i, logger)
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
	time.Sleep(150 * time.Second)
	for i := 0; i < num; i++ {
		err = sendNewMessage(t, conn, i, logger)
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
}

func sendNewMessage(t *testing.T, conn net.Conn, i int, logger *logrus.Entry) error {
	changes := map[string]int{ndtp.NphReqID: i}
	packetNav = ndtp.Change(packetNav, changes)
	return sendAndReceive(t, conn, packetNav, logger)
}

func mockTerminalEgtsStop(t *testing.T, addr string, num int) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_terminal"})
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer util.CloseAndLog(conn, logger)
	err = sendAndReceive(t, conn, packetAuth, logger)
	if err != nil {
		logger.Errorf("got error: %v", err)
		t.Error(err)
	}
	var i int
	logger.Println("SEND DATA1")
	for i = 0; i < num; i++ {
		err = sendNewMessage(t, conn, i, logger)
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
	logger.Println("SEND DATA1 END")
	/*
	time.Sleep(4 * time.Second)
	logger.Println("SEND DATA2")
	for ; i < num*2; i++ {
		err = sendNewMessage(t, conn, i, logger)
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
	logger.Println("SEND DATA2 END")
	time.Sleep(4 * time.Second)
	logger.Println("SEND DATA3")
	for ; i < num*3; i++ {
		err = sendNewMessage(t, conn, i, logger)
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
	logger.Println("SEND DATA3 END")*/
	time.Sleep(10 * time.Second)
}
