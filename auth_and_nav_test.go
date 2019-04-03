package main

import (
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)
import nav "github.com/ashirko/navprot"

var testCS string
var testFirstMessage chan struct{}
var testWaitNdtpServer chan struct{}

var id = 101210

var packetAuth = []byte{1, 2, 3, 126, 126, 59, 0, 2, 0, 14, 84, 2, 0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 1, 0, 0, 0, 0, 0, 6, 0, 2, 0, 2, 3, 90, 139, 1, 0, 0, 4, 0, 0, 0, 0, 0, 0, 51, 53, 53, 48, 57, 52, 48, 52, 51, 49, 56, 56, 51, 49, 49, 50, 53, 48, 48, 49, 54, 53, 48, 53, 56, 49, 53, 53, 51, 55, 0, 1, 2, 3}

var packetNav = []byte{0, 80, 86, 161, 44, 216, 192, 140, 96, 196, 138, 54, 8, 0, 69, 0, 0, 129, 102, 160, 64, 0, 125, 6,
	18, 51, 10, 68, 41, 150, 10, 176, 70, 26, 236, 153, 35, 56, 151, 147, 73, 96, 98, 94, 76, 40, 80,
	24, 1, 2, 190, 27, 0, 0, 126, 126, 74, 0, 2, 0, 107, 210, 2, 0, 0, 0, 0, 0, 0, 1, 0, 101, 0, 1, 0, 171,
	20, 0, 0, 0, 0, 36, 141, 198, 90, 87, 110, 119, 22, 201, 186, 64, 33, 224, 203, 0, 0, 0, 0, 83, 1, 0,
	0, 220, 0, 4, 0, 2, 0, 22, 0, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 167, 97, 0, 0, 31, 6, 0, 0, 8,
	0, 2, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5}

func TestOneNavMessage(t *testing.T) {
	defer clearDB(t)
	var wg sync.WaitGroup
	wg.Add(4)
	go testHandleConnection(t, &wg)
	go testNdtpServer(t, &wg)
	go testEgtsServer(t, &wg)
	go testNdtpClient(t, &wg)
	// check First Message is in db
	checkFirstMessage(t)

	wg.Wait()
}

func testInit() {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.InfoLevel)
	redisServer = ":9999"
	pool = newPool(redisServer)
	testCS = ":9001"
	ndtpServer = ":8001"
	egtsServer = ":7001"
	testFirstMessage = make(chan struct{})
	testWaitNdtpServer = make(chan struct{})
}

func testHandleConnection(t *testing.T, wg *sync.WaitGroup) {
	defer close(testFirstMessage)
	l, err := net.Listen("tcp", testCS)
	if err != nil {
		t.Error(err)
		return
	}
	close(testWaitNdtpServer)
	defer func() {
		if err = l.Close(); err != nil {
			t.Error(err)
		}
	}()
	c, err := l.Accept()
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		if err = c.Close(); err != nil {
			//t.Error(err)
		}
	}()
	handleConnection(c, 1)
	defer wg.Done()
}

func testNdtpClient(t *testing.T, wg *sync.WaitGroup) {
	defer wg.Done()
	<-testWaitNdtpServer
	conn, err := net.Dial("tcp", testCS)
	if err != nil {
		t.Error("error connect to testCS: ", err)
		return
	}
	defer func() {
		if err = conn.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err = sendMsgReciveRes(t, conn, packetAuth); err != nil {
		t.Error("error processing first message", err)
	}
	//close(step1)
	if err = sendMsgReciveRes(t, conn, packetNav); err != nil {
		t.Error("error processing nav message", err)
	}
}

func sendMsgReciveRes(t *testing.T, conn net.Conn, packet []byte) (err error) {
	if _, err = conn.Write(packet); err != nil {
		t.Error(err)
		return
	}
	var b [defaultBufferSize]byte
	if err = conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Error(err)
		return
	}
	n, err := conn.Read(b[:])
	if err != nil {
		t.Error(err)
		return
	}
	ndtp := new(nav.NDTP)
	_, err = ndtp.Parse(b[:n])
	if err != nil {
		t.Error(err)
		return
	}
	if !ndtp.IsResult() {
		t.Errorf("expected result from client")
	}
	return
}

func testNdtpServer(t *testing.T, wg *sync.WaitGroup) {
	defer wg.Done()
	l, err := net.Listen("tcp", ndtpServer)
	if err != nil {
		t.Error("can't listen ndtpServer")
		return
	}
	defer func() {
		if err = l.Close(); err != nil {
			t.Error(err)
		}
	}()
	conn, err := l.Accept()
	if err != nil {
		t.Error("error getting connection from CS: ", err)
		return
	}
	defer func() {
		if err = conn.Close(); err != nil {
			t.Error(err)
		}
	}()
	ndtp, err := getMsgAndReply(t, conn)
	if err != nil {
		t.Error("can't get auth packet")
		return
	}
	if ndtp.PacketType() != nav.NphSgsConnRequest {
		t.Errorf("expected auth packet")
	}
	ndtp1, err := getMsgAndReply(t, conn)
	if err != nil {
		t.Error("can't get nav packet")
		return
	}
	expected := []byte{126, 126, 74, 0, 2, 0, 139, 235, 2, 0, 0, 0, 0, 0, 0, 1, 0, 101, 0, 1, 0, 0, 0, 0, 0, 0, 0, 36, 141, 198, 90, 87, 110, 119, 22, 201, 186, 64, 33, 224, 203, 0, 0, 0, 0, 83, 1, 0, 0, 220, 0, 4, 0, 2, 0, 22, 0, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 167, 97, 0, 0, 31, 6, 0, 0, 8, 0, 2, 0, 0, 0, 0, 0}
	if !reflect.DeepEqual(ndtp1.Packet, expected) {
		t.Error("\nexpected: ", expected, "\n",
			"\ngot:      ", ndtp1.Packet)
	}
}

func getMsgAndReply(t *testing.T, conn net.Conn) (*nav.NDTP, error) {
	var b [defaultBufferSize]byte
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Error(err)
		return nil, err
	}
	n, err := conn.Read(b[:])
	if err != nil {
		t.Error(err)
		return nil, err
	}
	ndtp := new(nav.NDTP)
	_, err = ndtp.Parse(b[:n])
	if err != nil {
		t.Error(err)
		return nil, err
	}
	return ndtp, nil
}

func testEgtsServer(t *testing.T, wg *sync.WaitGroup) {
	defer wg.Done()
	l, err := net.Listen("tcp", egtsServer)
	if err != nil {
		t.Error("error listen egtsServer: ", err)
		return
	}
	go egtsServerSession()
	c, err := l.Accept()
	if err != nil {
		t.Error("error getting connection from CS: ", err)
		return
	}
	defer func() {
		if err = c.Close(); err != nil {
			t.Error(err)
		}
	}()
	var b [defaultBufferSize]byte
	if err = c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Error(err)
		return
	}
	n, err := c.Read(b[:])
	if err != nil {
		t.Error(err)
		return
	}
	egts := new(nav.EGTS)
	if _, err = egts.Parse(b[:n]); err != nil {
		t.Error(err)
	}
	egtsExpect := egtsExpected()
	if !reflect.DeepEqual(egtsExpect, egts) {
		t.Error("\nexpected: ", egtsExpect, "\ngot:      ", egts)
	}
}

func checkFirstMessage(t *testing.T) {
	<-testFirstMessage
	time.Sleep(50 * time.Millisecond)
	c := pool.Get()
	defer func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}()
	key := "conn:" + strconv.Itoa(id)
	res, err := redis.Bytes(c.Do("GET", key))
	if err != nil {
		t.Error(err)
	}
	expected := []byte{126, 126, 59, 0, 2, 0, 14, 84, 2, 127, 0, 0, 1, 0, 0, 0, 0, 100, 0, 1, 0, 0, 0, 0, 0, 6, 0, 2, 0, 2, 3, 90, 139, 1, 0, 0, 4, 0, 0, 0, 0, 0, 0, 51, 53, 53, 48, 57, 52, 48, 52, 51, 49, 56, 56, 51, 49, 49, 50, 53, 48, 48, 49, 54, 53, 48, 53, 56, 49, 53, 53, 51, 55, 0}
	if !reflect.DeepEqual(res, expected) {
		t.Error("\nexpected: ", expected, "\n",
			"\ngot:      ", res)
	}
}

func egtsExpected() *nav.EGTS {
	subrec := &nav.PosData{Time: 260657700, Lon: 37.6925782853953, Lat: 55.789024884763414, Bearing: 339, Valid: 1}
	rec := &nav.EgtsRecord{Service: 2, SubType: 16, Sub: subrec}
	return &nav.EGTS{PacketType: 1, ID: 101210, Data: rec}
}

func clearDB(t *testing.T) {
	c := pool.Get()
	defer func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}()
	_, err := c.Do("FLUSHALL")
	if err != nil {
		t.Error(err)
	}
}
