package test

import (
	"flag"
	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/server"
	"github.com/sirupsen/logrus"
	"testing"
	"time"
)

func Test_serverStartOne(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/one_server.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfMessages := 100
	go mockTerminal(t, "localhost:7000", numOfMessages)
	go mockNdtpMaster(t, "localhost:7001")
	go server.Start()
	time.Sleep(5 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfMessages + 3
	if len(res) != expected {
		t.Fatalf("expected %d keys in DB. Got %d: %v", expected, len(res), res)
	}
	time.Sleep(20 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = 3
	if len(res) != expected {
		t.Fatalf("expected %d keys in DB. Got %d: %v", expected, len(res), res)
	}
}

func Test_serverStartTwoTerminals(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/one_server_multiple_terminals.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfMessages := 2
	go mockTerminal(t, "localhost:7010", numOfMessages)
	go mockTerminalSecond(t, "localhost:7010", numOfMessages)
	go mockNdtpMaster(t, "localhost:7011")
	go server.Start()
	time.Sleep(5 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := (numOfMessages + 3) * 2
	if len(res) != expected {
		t.Fatalf("expected %d keys in DB. Got %d: %v", expected, len(res), res)
	}
	time.Sleep(20 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = 3 * 2
	if len(res) != expected {
		t.Fatalf("expected %d keys in DB. Got %d: %v", expected, len(res), res)
	}
}

func Test_serverStartTwo(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/two_servers.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfMessages := 100
	go mockTerminal(t, "localhost:7020", numOfMessages)
	go mockNdtpMaster(t, "localhost:7021")
	go mockNdtpServer(t, "localhost:7022")
	go server.Start()
	time.Sleep(5 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfMessages*2 + 4
	if len(res) != expected {
		t.Fatalf("expected %d keys in DB. Got %d: %v", expected, len(res), res)
	}
	time.Sleep(20 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = 4
	if len(res) != expected {
		t.Fatalf("expected %d keys in DB. Got %d: %v", expected, len(res), res)
	}
}

func Test_serverStartThree(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/three_servers.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfMessages := 2
	go mockTerminal(t, "localhost:7030", numOfMessages)
	go mockNdtpMaster(t, "localhost:7031")
	go mockNdtpServer(t, "localhost:7032")
	go mockEgtsServer(t, "localhost:7033")
	go server.Start()
	time.Sleep(5 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfMessages*3 + 5
	if len(res) != expected {
		t.Fatalf("expected %d keys in DB. Got %d: %v", expected, len(res), res)
	}
	time.Sleep(20 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = 5
	if len(res) != expected {
		t.Fatalf("expected %d keys in DB. Got %d: %v", expected, len(res), res)
	}
}
