package main

// 1) Att -> CS -> DB (auth)
// 2) Att <- CS (auth reply)
// 3) CS -> NDTP server (auth)

// 4) Att -> CS -> DB (nav)
// 5) Att <- CS (nav reply)

import (
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"log"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"
)

import nav "github.com/ashirko/navprot"

var testCS string

//var step1 chan struct{}
var id = 101210

var packetAuth = []byte{1, 2, 3, 126, 126, 59, 0, 2, 0, 14, 84, 2, 0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 1, 0, 0, 0, 0, 0, 6, 0, 2, 0, 2, 3, 90, 139, 1, 0, 0, 4, 0, 0, 0, 0, 0, 0, 51, 53, 53, 48, 57, 52, 48, 52, 51, 49, 56, 56, 51, 49, 49, 50, 53, 48, 48, 49, 54, 53, 48, 53, 56, 49, 53, 53, 51, 55, 0, 1, 2, 3}

var packetNav = []byte{0, 80, 86, 161, 44, 216, 192, 140, 96, 196, 138, 54, 8, 0, 69, 0, 0, 129, 102, 160, 64, 0, 125, 6,
	18, 51, 10, 68, 41, 150, 10, 176, 70, 26, 236, 153, 35, 56, 151, 147, 73, 96, 98, 94, 76, 40, 80,
	24, 1, 2, 190, 27, 0, 0, 126, 126, 74, 0, 2, 0, 107, 210, 2, 0, 0, 0, 0, 0, 0, 1, 0, 101, 0, 1, 0, 171,
	20, 0, 0, 0, 0, 36, 141, 198, 90, 87, 110, 119, 22, 201, 186, 64, 33, 224, 203, 0, 0, 0, 0, 83, 1, 0,
	0, 220, 0, 4, 0, 2, 0, 22, 0, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 167, 97, 0, 0, 31, 6, 0, 0, 8,
	0, 2, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5}

func TestTcpmirror(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.InfoLevel)
	redisServer = ":9999"
	pool = newPool(redisServer)
	defer pool.Close()
	defer clearDB(t)
	//step1 = make(chan struct{})
	testCS = ":9001"
	l, err := net.Listen("tcp", testCS)
	if err != nil {
		t.Fatal(err)
	}
	ndtpServer = ":8001"
	egtsServer = ":7001"
	go testNdtpServer(t)
	go testEgtsServer(t)
	time.Sleep(100 * time.Millisecond)
	go testNdtpClient(t)
	c, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	go handleConnection(c, 1)
	// check First Message is in db
	checkFirstMessage(t)
	//
	time.Sleep(100 * time.Millisecond)
}

func testNdtpClient(t *testing.T) {
	//time.Sleep(1 * time.Millisecond)
	conn, err := net.Dial("tcp", testCS)
	if err != nil {
		t.Fatal("error connect to testCS: ", err)
	}
	defer conn.Close()
	_, err = conn.Write(packetAuth)
	if err != nil {
		t.Fatal(err)
	}
	var b [defaultBufferSize]byte
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(b[:])
	if err != nil {
		t.Fatal(err)
	}
	ndtp := new(nav.NDTP)
	_, err = ndtp.Parse(b[:n])
	if err != nil {
		t.Fatal(err)
	}
	if !ndtp.IsResult() {
		t.Errorf("expected result from client")
	}
	//close(step1)
	_, err = conn.Write(packetNav)
	if err != nil {
		t.Fatal(err)
	}
	var b1 [defaultBufferSize]byte
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err = conn.Read(b1[:])
	if err != nil {
		t.Fatal(err)
	}
	ndtp1 := new(nav.NDTP)
	_, err = ndtp1.Parse(b1[:n])
	if err != nil {
		t.Fatal(err)
	}
	if !ndtp1.IsResult() {
		t.Errorf("expected result from client")
	}
	time.Sleep(4 * time.Second)
}

func testNdtpServer(t *testing.T) {
	l, err := net.Listen("tcp", ndtpServer)
	if err != nil {
		t.Fatalf("can't listen ndtpServer")
	}
	conn, err := l.Accept()
	if err != nil {
		t.Fatal("error getting connection from CS: ", err)
	}
	defer conn.Close()
	var b [defaultBufferSize]byte
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(b[:])
	log.Println("read ", n)
	if err != nil {
		t.Fatal(err)
	}
	ndtp := new(nav.NDTP)
	_, err = ndtp.Parse(b[:n])
	if err != nil {
		t.Fatal(err)
	}
	if ndtp.PacketType() != nav.NphSgsConnRequest {
		t.Errorf("expected auth packet")
	}
	var b1 [defaultBufferSize]byte
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err = conn.Read(b1[:])
	log.Println("read ", n)
	if err != nil {
		t.Fatal(err)
	}

	ndtp1 := new(nav.NDTP)
	_, err = ndtp1.Parse(b1[:n])
	if err != nil {
		t.Fatal(err)
	}
	expected := []byte{126, 126, 74, 0, 2, 0, 139, 235, 2, 0, 0, 0, 0, 0, 0, 1, 0, 101, 0, 1, 0, 0, 0, 0, 0, 0, 0, 36, 141, 198, 90, 87, 110, 119, 22, 201, 186, 64, 33, 224, 203, 0, 0, 0, 0, 83, 1, 0, 0, 220, 0, 4, 0, 2, 0, 22, 0, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 167, 97, 0, 0, 31, 6, 0, 0, 8, 0, 2, 0, 0, 0, 0, 0}
	if !reflect.DeepEqual(ndtp1.Packet, expected) {
		t.Error("\nexpected: ", expected, "\n",
			"\ngot:      ", ndtp1.Packet)
	}
	time.Sleep(4 * time.Second)
}

func testEgtsServer(t *testing.T) {
	l, err := net.Listen("tcp", egtsServer)
	if err != nil {
		t.Fatal("error listen egtsServer: ", err)
	}
	c, err := l.Accept()
	if err != nil {
		t.Fatal("error getting connection from CS: ", err)
	}
	defer c.Close()
}

func checkFirstMessage(t *testing.T) {
	time.Sleep(50 * time.Millisecond)
	c := pool.Get()
	defer c.Close()
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

func clearDB(t *testing.T) {
	c := pool.Get()
	defer c.Close()
	_, err := c.Do("FLUSHALL")
	if err != nil {
		t.Error(err)
	}
}
