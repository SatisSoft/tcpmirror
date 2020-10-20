package test

import (
	"net"
	"testing"
	"time"

	"github.com/egorban/navprot/pkg/ndtp"
	"github.com/sirupsen/logrus"
)

const (
	defaultBufferSize = 65536 //1024
	writeTimeout      = 10 * time.Second
)

var packetControl = []byte{126, 126, 136, 1, 2, 0, 135, 235, 2, 0, 4, 0, 0, 176, 1, 5, 0, 100, 0, 0, 0, 17, 0, 0, 0, 16, 0, 0, 128, 0, 0, 0, 0, 1, 0, 0, 0, 60, 78, 65, 86, 83, 67, 82, 32, 118, 101, 114, 61, 49, 46, 48, 62, 60, 73, 68, 62, 49, 54, 60, 47, 73, 68, 62, 60, 70, 82, 79, 77, 62, 83, 69, 82, 86, 69, 82, 60, 47, 70, 82, 79, 77, 62, 60, 84, 79, 62, 85, 83, 69, 82, 60, 47, 84, 79, 62, 60, 84, 89, 80, 69, 62, 81, 85, 69, 82, 89, 60, 47, 84, 89, 80, 69, 62, 60, 77, 83, 71, 32, 116, 105, 109, 101, 61, 49, 56, 48, 48, 32, 116, 121, 112, 101, 61, 98, 97, 99, 107, 103, 114, 111, 117, 110, 100, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 53, 56, 48, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 50, 48, 53, 38, 110, 98, 115, 112, 59, 49, 38, 110, 98, 115, 112, 59, 43, 48, 50, 60, 98, 114, 32, 47, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 204, 229, 242, 240, 238, 38, 110, 98, 115, 112, 59, 208, 255, 231, 224, 237, 241, 234, 232, 38, 110, 98, 115, 112, 59, 49, 51, 58, 48, 51, 60, 98, 114, 32, 47, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 204, 229, 242, 240, 238, 38, 110, 98, 115, 112, 59, 194, 251, 245, 232, 237, 238, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 49, 51, 58, 49, 49, 60, 98, 114, 32, 47, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 196, 229, 242, 241, 234, 224, 255, 38, 110, 98, 115, 112, 59, 248, 234, 238, 235, 224, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 49, 51, 58, 49, 55, 60, 98, 114, 32, 47, 62, 60, 47, 77, 83, 71, 62, 60, 47, 78, 65, 86, 83, 67, 82, 62}

var packetControlReply = []byte{126, 126, 18, 0, 2, 0, 95, 251, 2, 0, 4, 0, 0, 0, 0, 5, 0, 102, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0}

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
