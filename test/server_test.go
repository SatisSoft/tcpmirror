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
	numOfPackets := 10
	numOfNdtpServers := 1
	numOfTerminals := 1
	go mockTerminal(t, "localhost:7000", numOfPackets)
	go mockNdtpMaster(t, "localhost:7001")
	go server.Start()
	time.Sleep(3 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfPackets*numOfNdtpServers*numOfTerminals
	logrus.Println("start 1 test")
	checkKeyNum(t, res, expected)
	time.Sleep(3 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals
	logrus.Println("start 2 test")
    checkKeyNum(t, res, expected)
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
	numOfPackets := 10
	numOfNdtpServers := 1
	numOfTerminals := 2
	go mockTerminal(t, "localhost:7010", numOfPackets)
	go mockTerminalSecond(t, "localhost:7010", numOfPackets)
	go mockNdtpMaster(t, "localhost:7011")
	go server.Start()
	time.Sleep(3 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfPackets*numOfNdtpServers*numOfTerminals
	logrus.Println("start 1 test")
    checkKeyNum(t, res, expected)
	time.Sleep(3 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals
	logrus.Println("start 2 test")
    checkKeyNum(t, res, expected)
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
	numOfPackets := 10
	numOfNdtpServers := 2
	numOfTerminals := 1
	go mockTerminal(t, "localhost:7020", numOfPackets)
	go mockNdtpMaster(t, "localhost:7021")
	go mockNdtpServer(t, "localhost:7022")
	go server.Start()
	time.Sleep(3 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfPackets*numOfNdtpServers*numOfTerminals
	logrus.Println("start 1 test")
    checkKeyNum(t, res, expected)
	time.Sleep(3 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals
	logrus.Println("start 2 test")
    checkKeyNum(t, res, expected)
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
	numOfPackets := 10
	numOfNdtpServers := 2
	numOfEgtsServers := 1
	numOfTerminals := 1
	go mockTerminal(t, "localhost:7030", numOfPackets)
	go mockNdtpMaster(t, "localhost:7031")
	go mockNdtpServer(t, "localhost:7032")
	go mockEgtsServer(t, "localhost:7033")
	go server.Start()
	time.Sleep(3 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals*2 + (numOfNdtpServers+numOfEgtsServers)*numOfTerminals + numOfPackets*(numOfNdtpServers+numOfEgtsServers)*numOfTerminals
	logrus.Println("start 1 test")
    checkKeyNum(t, res, expected)
	time.Sleep(3 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfEgtsServers
	logrus.Println("start 2 test")
    checkKeyNum(t, res, expected)
}

func Test_serverStartOneNotMaster(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/one_server_not_master.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfPackets := 10
	numOfNdtpServers := 1
	numOfTerminals := 1
	numOfEgtsServers := 1
	notConfirmed := 1
	go mockTerminal(t, "localhost:7040", numOfPackets)
	go mockNdtpServer(t, "localhost:7042")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals*(2+notConfirmed) + (numOfNdtpServers)*numOfTerminals + numOfEgtsServers +
		numOfPackets*(numOfNdtpServers+notConfirmed)*numOfTerminals
	logrus.Println("start 1 test")
    checkKeyNum(t, res, expected)
	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*(2+notConfirmed) + (numOfNdtpServers)*numOfTerminals +
		numOfPackets*numOfTerminals + numOfEgtsServers*notConfirmed
	logrus.Println("start 2 test")
    checkKeyNum(t, res, expected)
}

func Test_serverStartOneGuaranteedDelivery(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/one_server_guaranteed_delivery.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfPackets := 10
	numOfNdtpServers := 1
	numOfTerminals := 1
	notConfirmed := 1
	numOfEgtsServers := 1
	go mockTerminalGuaranteedDeliveryMaster(t, "localhost:7050", numOfPackets, 1)
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals*(2+notConfirmed) + numOfEgtsServers +
		numOfPackets*notConfirmed*numOfTerminals
	logrus.Println("start 1 test")
    checkKeyNum(t, res, expected)
	time.Sleep(8 * time.Second)
	go mockTerminalGuaranteedDeliveryMaster(t, "localhost:7050", 0, 30)
	go mockNdtpMaster(t, "localhost:7051")
	time.Sleep(11 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals
	logrus.Println("start 2 test")
    checkKeyNum(t, res, expected)
}

func Test_serverStartTwoGuaranteedDelivery(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/two_servers_guaranteed_delivery.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfPackets := 10
	numOfNdtpServers := 2
	numOfTerminals := 1
	notConfirmed := 1
	numOfEgtsServers := 1
	go mockTerminalGuaranteedDeliveryMaster(t, "localhost:7060", numOfPackets, 1)
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals*(2+notConfirmed) + numOfEgtsServers +
		numOfPackets*notConfirmed*numOfTerminals
	logrus.Println("start 1 test")
    checkKeyNum(t, res, expected)
	time.Sleep(8 * time.Second)
	logrus.Println("START NDTP SERVERS")
	go mockTerminalGuaranteedDeliveryMaster(t, "localhost:7060", numOfPackets, 30)
	go mockNdtpMaster(t, "localhost:7061")
	go mockNdtpServer(t, "localhost:7062")
	time.Sleep(11 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals
	logrus.Println("start 2 test")
    checkKeyNum(t, res, expected)
}

func Test_serverStartThreeEgtsDisconnect(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/three_servers_egts_disconnect.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfPackets := 10
	numOfNdtpServers := 2
	numOfEgtsServers := 1
	numOfTerminals := 1
	go mockTerminalEgtsStop(t, "localhost:7070", numOfPackets)
	go mockNdtpMaster(t, "localhost:7071")
	go mockNdtpServer(t, "localhost:7072")
	go mockEgtsServer(t, "localhost:7073")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals*2 + (numOfNdtpServers+numOfEgtsServers)*numOfTerminals + numOfPackets*(numOfNdtpServers+numOfEgtsServers)*numOfTerminals
	logrus.Println("start 1 test")
	checkKeyNum(t, res, expected)
	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfEgtsServers
	logrus.Println("start 2 test")
	checkKeyNum(t, res, expected)
	mockEgtsServerStop(t)
	time.Sleep(8 * time.Second)
	res, err = getAllKeys(conn)

	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfEgtsServers + numOfTerminals + numOfPackets + 1
	logrus.Println("start 3 test")
	checkKeyNum(t, res, expected)
	go mockEgtsServer(t, "localhost:7073")
	time.Sleep(12 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfEgtsServers //+ numOfPackets*numOfTerminals
	logrus.Println("start 4 test")
	checkKeyNum(t, res, expected)

}

func checkKeyNum(t *testing.T, res [][]byte, expected int) {
	if len(res) != expected {
		t.Fatalf("expected %d keys in DB. Got %d: %s", expected, len(res), res)
	}
}

func Test_controlMessage(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/control_message.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfPackets := 0
	numOfNdtpServers := 1
	numOfTerminals := 1
	go mockTerminalWithControl(t, "localhost:6080", numOfPackets)
	go mockNdtpMasterWithControl(t, "localhost:6081")
	go server.Start()
	time.Sleep(3 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals + numOfNdtpServers*numOfTerminals + numOfPackets*numOfNdtpServers*numOfTerminals
	logrus.Println("start 1 test")
    checkKeyNum(t, res, expected)
}

func Test_serverStartAllOff(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/all_off.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfPackets := 10
	numOfNdtpServers := 2
	numOfEgtsServers := 1
	numOfTerminals := 1
	go mockTerminalAllOff(t, "localhost:7080", numOfPackets)
	go server.Start()
	time.Sleep(1 * time.Second)
	res, err := getAllKeys(conn)
    if err != nil {
    	t.Fatal(err)
    }
    expected := numOfTerminals*2 + numOfTerminals + numOfPackets + 1
    logrus.Println("start 1 test")
    checkKeyNum(t, res, expected)
    time.Sleep(8 * time.Second)
    go mockTerminalAllOff(t, "localhost:7080", 0)
	go mockNdtpMaster(t, "localhost:7082")
    go mockNdtpServer(t, "localhost:7083")
    go mockEgtsServer(t, "localhost:7081")
    time.Sleep(11 * time.Second)
    res, err = getAllKeys(conn)
    if err != nil {
    	t.Fatal(err)
    }
    expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfEgtsServers
    logrus.Println("start 2 test")
    checkKeyNum(t, res, expected)
}

func Test_serverStartThreeNdtp3(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/three_servers_ndtp3.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfPackets := 10
	numOfNdtpServers := 3
	numOfEgtsServers := 1
	numOfTerminals := 1
	go mockTerminal(t, "localhost:7090", numOfPackets)
	go mockNdtpMaster(t, "localhost:7092")
	go mockNdtpServer(t, "localhost:7093")
	go mockNdtpServer(t, "localhost:7094")
	go mockEgtsServer(t, "localhost:7091")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals*2 + (numOfNdtpServers+numOfEgtsServers)*numOfTerminals + numOfPackets*(numOfNdtpServers+numOfEgtsServers)*numOfTerminals
	logrus.Println("start 1 test")
	checkKeyNum(t, res, expected)
	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfEgtsServers
	logrus.Println("start 2 test")
	checkKeyNum(t, res, expected)
}

func Test_serverStartThreeEgts3(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/three_servers_egts3.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfPackets := 10
	numOfNdtpServers := 2
	numOfEgtsServers := 3
	numOfTerminals := 1
	go mockTerminal(t, "localhost:7100", numOfPackets)
	go mockNdtpMaster(t, "localhost:7104")
	go mockNdtpServer(t, "localhost:7105")
	go mockEgtsServer(t, "localhost:7101")
	go mockEgtsServer(t, "localhost:7102")
	go mockEgtsServer(t, "localhost:7103")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals*2 + (numOfNdtpServers+numOfEgtsServers)*numOfTerminals + numOfPackets*(numOfNdtpServers+numOfEgtsServers)*numOfTerminals
	logrus.Println("start 1 test")
	checkKeyNum(t, res, expected)
	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfEgtsServers
	logrus.Println("start 2 test")
	checkKeyNum(t, res, expected)
}

func Test_serverStartThreeTerminals100(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/three_servers_terminals100.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfPackets := 10
	numOfNdtpServers := 2
	numOfEgtsServers := 1
	numOfTerminals := 3
	for i := 0; i < numOfTerminals; i++ {
	    go mockTerminals100(t, "localhost:7200", numOfPackets, i)
	}
	go mockNdtpMaster(t, "localhost:7201")
	go mockNdtpServer(t, "localhost:7202")
	go mockEgtsServer(t, "localhost:7203")
	go server.Start()
	time.Sleep(20 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfEgtsServers + numOfPackets*(numOfNdtpServers+numOfEgtsServers)*numOfTerminals
	logrus.Println("start 1 test")
    checkKeyNum(t, res, expected)

	time.Sleep(20 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfEgtsServers
	logrus.Println("start 2 test")
    checkKeyNum(t, res, expected)

}

func Test_serverStartThreeNdtp3Egts3Terminals100(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/three_servers_ndtp3_egts3_terminals100.toml")
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Fatal(err)
	}
	numOfPackets := 10
	numOfNdtpServers := 3
	numOfEgtsServers := 3
	numOfTerminals := 3
	for i := 0; i < numOfTerminals; i++ {
	    go mockTerminals100(t, "localhost:7300", numOfPackets, i)
	}
	go mockNdtpMaster(t, "localhost:7304")
	go mockNdtpServer(t, "localhost:7305")
	go mockNdtpServer(t, "localhost:7306")
	go mockEgtsServer(t, "localhost:7301")
	go mockEgtsServer(t, "localhost:7302")
	go mockEgtsServer(t, "localhost:7303")
	go server.Start()
	time.Sleep(20 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected := numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfEgtsServers + numOfPackets*(numOfNdtpServers+numOfEgtsServers)*numOfTerminals
	logrus.Println("start 1 test")
    checkKeyNum(t, res, expected)

	time.Sleep(20 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Fatal(err)
	}
	expected = numOfTerminals*2 + numOfNdtpServers*numOfTerminals + numOfEgtsServers
	logrus.Println("start 2 test")
    checkKeyNum(t, res, expected)
}
