package test

import (
	"github.com/ashirko/navprot/pkg/ndtp"
	"github.com/sirupsen/logrus"
	"net"
	"testing"
	"time"
)

const (
	defaultBufferSize = 1024
	writeTimeout      = 10 * time.Second
	readTimeout       = 180 * time.Second
)

func sendAndReceive(t *testing.T, c net.Conn, packet []byte, logger *logrus.Entry) (err error) {
	err = send(c, packet)
	logger.Tracef("send: %v", packet)
	if err != nil {
		return
	}
	var b [defaultBufferSize]byte
	n, err := c.Read(b[:])
	logger.Tracef("receive: %v", b[:n])
	return
}

func receiveAndReply(t *testing.T, c net.Conn, logger *logrus.Entry) (err error) {
	var b [defaultBufferSize]byte
	n, err := c.Read(b[:])
	if err != nil {
		return
	}
	p := new(ndtp.Packet)
	_, err = p.Parse(b[:n])
	logger.Println("received:", p.Packet)
	if err != nil {
		return
	}
	rep := p.Reply(0)
	logger.Println("KATYA SEND REPLY",rep)
	logger.Println("send:", rep)
	err = send(c, rep)
	if err != nil {
		return
	}
	return
}

func send(conn net.Conn, packet []byte) error {
	err := conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err != nil {
		return err
	}
	_, err = conn.Write(packet)
	return err
}
