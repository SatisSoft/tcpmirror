package test

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/egorban/navprot/pkg/ndtp"
	"github.com/sirupsen/logrus"
)

var packetAuth = []byte{126, 126, 59, 0, 2, 0, 14, 84, 2, 0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 1, 0, 0, 0, 0, 0, 6, 0, 2, 0, 2, 3, 90, 139, 1, 0, 0, 4, 0, 0, 0, 0, 0, 0, 51, 53, 53, 48, 57, 52, 48, 52, 51, 49, 56, 56, 51, 49, 49, 50, 53, 48, 48, 49, 54, 53, 48, 53, 56, 49, 53, 53, 51, 55, 0}
var packetAuthSecond = []byte{1, 2, 3, 126, 126, 59, 0, 2, 0, 222, 186, 2, 0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 1, 0, 0, 0, 0, 0, 6, 0, 2, 0, 2, 3, 90, 128, 1, 0, 0, 4, 0, 0, 0, 0, 0, 0, 51, 53, 53, 48, 57, 52, 48, 52, 51, 49, 56, 56, 51, 49, 49, 50, 53, 48, 48, 49, 54, 53, 48, 53, 56, 49, 53, 53, 51, 55, 0}

var packetNav = []byte{126, 126, 74, 0, 2, 0, 107, 210, 2, 0, 0, 0, 0, 0, 0, 1, 0, 101, 0, 1, 0, 171,
	20, 0, 0, 0, 0, 36, 141, 198, 90, 87, 110, 119, 22, 201, 186, 64, 33, 224, 203, 0, 0, 0, 0, 83, 1, 0,
	0, 220, 0, 4, 0, 2, 0, 22, 0, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 167, 97, 0, 0, 31, 6, 0, 0, 8,
	0, 2, 0, 0, 0, 0, 0}

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

func mockTerminalWithControl(t *testing.T, addr string, num int) {
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
	err = receiveAndReplyControl(t, conn, logger)
	if err != nil {
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

func receiveAndReplyControl(t *testing.T, c net.Conn, logger *logrus.Entry) (err error) {
	var b [defaultBufferSize]byte
	n, err := c.Read(b[:])
	if err != nil {
		return
	}
	if !bytes.Equal(b[:n], packetControl) {
		t.Errorf("expected control packet but received %s", b[:n])
	}
	p := new(ndtp.Packet)
	_, err = p.Parse(b[:n])
	logger.Println("received:", p.Packet)
	if err != nil {
		return
	}
	logger.Println("send:", packetControlReply)
	err = send(c, packetControlReply)
	if err != nil {
		return
	}
	return
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
	for i = 0; i < num; i++ {
		err = sendNewMessage(t, conn, i, logger)
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
	time.Sleep(4 * time.Second)
	for ; i < num*2; i++ {
		err = sendNewMessage(t, conn, i, logger)
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
	time.Sleep(8 * time.Second)
	for ; i < num*3; i++ {
		err = sendNewMessage(t, conn, i, logger)
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
	time.Sleep(30 * time.Second)
}

func mockTerminalAllOff(t *testing.T, addr string, num int) {
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
	for i = 0; i < num; i++ {
		err = sendNewMessage(t, conn, i, logger)
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
	time.Sleep(30 * time.Second)
}

func mockTerminals100(t *testing.T, addr string, num int, terminalID int) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_terminal"})
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer util.CloseAndLog(conn, logger)
	changes := map[string]int{ndtp.PeerAddress: terminalID}
	newPacketAuth := ndtp.Change(packetAuth, changes)
	err = sendAndReceive(t, conn, newPacketAuth, logger)
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
	time.Sleep(100 * time.Second)
}
