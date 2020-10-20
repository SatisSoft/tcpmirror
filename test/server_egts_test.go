package test

import (
	"flag"
	"testing"
	"time"

	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/server"
	"github.com/sirupsen/logrus"
)

func Test_OneSourceOneIDOneServerEGTS(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/egts_one_source_one_id_one_server.toml")
	if err != nil {
		t.Error(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Error(err)
	}
	numEgtsSource := 1
	numOfOids := uint32(1)
	numOfEgtsServers := 1
	numOfRecs := 20
	for i := 0; i < numEgtsSource; i++ {
		go mockSourceEgts(t, "localhost:7400", numOfRecs, numOfOids)
	}
	go mockEgtsServer(t, "localhost:7401")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected := 1 + numOfEgtsServers + numOfEgtsServers*numOfRecs*numEgtsSource
	logrus.Println("start 1 test", expected, len(res))
	checkKeyNum(t, res, expected)

	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected = 1 + numOfEgtsServers
	logrus.Println("start 2 test", expected, len(res))
	checkKeyNum(t, res, expected)
}

func Test_OneSourceOneIDThreeServer(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/egts_one_source_one_id_three_servers.toml")
	if err != nil {
		t.Error(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Error(err)
	}
	numEgtsSource := 1
	numOfOids := uint32(1)
	numOfEgtsServers := 3
	numOfRecs := 20
	for i := 0; i < numEgtsSource; i++ {
		go mockSourceEgts(t, "localhost:7405", numOfRecs, numOfOids)
	}
	go mockEgtsServer(t, "localhost:7406")
	go mockEgtsServer(t, "localhost:7407")
	go mockEgtsServer(t, "localhost:7408")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected := 1 + numOfEgtsServers + numOfEgtsServers*numOfRecs*numEgtsSource
	logrus.Println("start 1 test", expected, len(res))
	checkKeyNum(t, res, expected)

	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected = 1 + numOfEgtsServers
	logrus.Println("start 2 test", expected, len(res))
	checkKeyNum(t, res, expected)
}

func Test_OneSourceSeveralIDOneServer(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/egts_one_source_several_id_one_server.toml")
	if err != nil {
		t.Error(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Error(err)
	}
	numEgtsSource := 1
	numOfOids := uint32(10)
	numOfEgtsServers := 1
	numOfRecs := 20
	for i := 0; i < numEgtsSource; i++ {
		go mockSourceEgts(t, "localhost:7410", numOfRecs, numOfOids)
	}
	go mockEgtsServer(t, "localhost:7411")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected := 1 + numOfEgtsServers + numOfEgtsServers*numOfRecs*numEgtsSource
	logrus.Println("start 1 test", expected, len(res))
	checkKeyNum(t, res, expected)

	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected = 1 + numOfEgtsServers
	logrus.Println("start 2 test", expected, len(res))
	checkKeyNum(t, res, expected)
}

func Test_OneSourceSeveralIDThreeServer(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/egts_one_source_several_id_three_servers.toml")
	if err != nil {
		t.Error(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Error(err)
	}
	numEgtsSource := 1
	numOfOids := uint32(10)
	numOfEgtsServers := 3
	numOfRecs := 20
	for i := 0; i < numEgtsSource; i++ {
		go mockSourceEgts(t, "localhost:7415", numOfRecs, numOfOids)
	}
	go mockEgtsServer(t, "localhost:7416")
	go mockEgtsServer(t, "localhost:7417")
	go mockEgtsServer(t, "localhost:7418")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected := 1 + numOfEgtsServers + numOfEgtsServers*numOfRecs*numEgtsSource
	logrus.Println("start 1 test", expected, len(res))
	checkKeyNum(t, res, expected)

	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected = 1 + numOfEgtsServers
	logrus.Println("start 2 test", expected, len(res))
	checkKeyNum(t, res, expected)
}

func Test_ThreeSourceOneIDOneServer(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/egts_three_source_one_id_one_server.toml")
	if err != nil {
		t.Error(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Error(err)
	}
	numEgtsSource := 3
	numOfOids := uint32(1)
	numOfEgtsServers := 1
	numOfRecs := 20
	for i := 0; i < numEgtsSource; i++ {
		go mockSourceEgts(t, "localhost:7420", numOfRecs, numOfOids)
	}
	go mockEgtsServer(t, "localhost:7421")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected := 1 + numOfEgtsServers + numOfEgtsServers*numOfRecs*numEgtsSource
	logrus.Println("start 1 test", expected, len(res))
	checkKeyNum(t, res, expected)

	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected = 1 + numOfEgtsServers
	logrus.Println("start 2 test", expected, len(res))
	checkKeyNum(t, res, expected)
}

func Test_ThreeSourceOneIDThreeServer(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/egts_three_source_one_id_three_servers.toml")
	if err != nil {
		t.Error(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Error(err)
	}
	numEgtsSource := 3
	numOfOids := uint32(1)
	numOfEgtsServers := 3
	numOfRecs := 20
	for i := 0; i < numEgtsSource; i++ {
		go mockSourceEgts(t, "localhost:7425", numOfRecs, numOfOids)
	}
	go mockEgtsServer(t, "localhost:7426")
	go mockEgtsServer(t, "localhost:7427")
	go mockEgtsServer(t, "localhost:7428")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected := 1 + numOfEgtsServers + numOfEgtsServers*numOfRecs*numEgtsSource
	logrus.Println("start 1 test", expected, len(res))
	checkKeyNum(t, res, expected)

	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected = 1 + numOfEgtsServers
	logrus.Println("start 2 test", expected, len(res))
	checkKeyNum(t, res, expected)
}

func Test_ThreeSourceSeveralIDOneServer(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/egts_three_source_several_id_one_server.toml")
	if err != nil {
		t.Error(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Error(err)
	}
	numEgtsSource := 3
	numOfOids := uint32(10)
	numOfEgtsServers := 1
	numOfRecs := 20
	for i := 0; i < numEgtsSource; i++ {
		go mockSourceEgts(t, "localhost:7430", numOfRecs, numOfOids)
	}
	go mockEgtsServer(t, "localhost:7431")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected := 1 + numOfEgtsServers + numOfEgtsServers*numOfRecs*numEgtsSource
	logrus.Println("start 1 test", expected, len(res))
	checkKeyNum(t, res, expected)

	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected = 1 + numOfEgtsServers
	logrus.Println("start 2 test", expected, len(res))
	checkKeyNum(t, res, expected)
}

func Test_ThreeSourceSeveralIDThreeServer(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/egts_three_source_several_id_three_servers.toml")
	if err != nil {
		t.Error(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Error(err)
	}
	numEgtsSource := 3
	numOfOids := uint32(10)
	numOfEgtsServers := 3
	numOfRecs := 20
	for i := 0; i < numEgtsSource; i++ {
		go mockSourceEgts(t, "localhost:7435", numOfRecs, numOfOids)
	}
	go mockEgtsServer(t, "localhost:7436")
	go mockEgtsServer(t, "localhost:7437")
	go mockEgtsServer(t, "localhost:7438")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected := 1 + numOfEgtsServers + numOfEgtsServers*numOfRecs*numEgtsSource
	logrus.Println("start 1 test", expected, len(res))
	checkKeyNum(t, res, expected)

	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected = 1 + numOfEgtsServers
	logrus.Println("start 2 test", expected, len(res))
	checkKeyNum(t, res, expected)
}

func Test_ThreeSourceSeveralIDThreeServerOff(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/egts_three_servers_off.toml")
	if err != nil {
		t.Error(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Error(err)
	}
	numEgtsSource := 3
	numOfOids := 10
	numOfEgtsServers := 3
	numOfRecs := 20
	for i := 0; i < numEgtsSource; i++ {
		go mockSourceEgts(t, "localhost:7440", numOfRecs, uint32(numOfOids))
	}
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected := 1 + 1 + 1 + numEgtsSource*numOfRecs //1 + numEgtsSource*numOfRecs + 1 + numOfOids
	logrus.Println("start 1 test", expected, len(res))
	checkKeyNum(t, res, expected)

	time.Sleep(8 * time.Second)
	go mockEgtsServer(t, "localhost:7441")
	go mockEgtsServer(t, "localhost:7442")
	go mockEgtsServer(t, "localhost:7443")
	time.Sleep(15 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected = 1 + numOfEgtsServers
	logrus.Println("start 2 test", expected, len(res))
	checkKeyNum(t, res, expected)
}

func Test_OneSourceSeveralIDOneServerDisconnect(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	err := flag.Set("conf", "./testconfig/egts_one_server_disconnect.toml")
	if err != nil {
		t.Error(err)
	}
	conn := db.Connect("localhost:9999")
	if err := clearDB(conn); err != nil {
		t.Error(err)
	}
	numEgtsSource := 1
	numOfOids := 10
	numOfEgtsServers := 1
	numOfRecs := 20
	for i := 0; i < numEgtsSource; i++ {
		go mockSourceEgts(t, "localhost:7445", numOfRecs, uint32(numOfOids))
	}
	go mockEgtsServer(t, "localhost:7446")
	go server.Start()
	time.Sleep(2 * time.Second)
	res, err := getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected := 1 + numOfEgtsServers + numOfEgtsServers*numOfRecs*numEgtsSource
	logrus.Println("start 1 test", expected, len(res))
	checkKeyNum(t, res, expected)

	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected = 1 + numOfEgtsServers
	logrus.Println("start 2 test", expected, len(res))
	checkKeyNum(t, res, expected)

	go mockEgtsServerStop(t)
	for i := 0; i < numEgtsSource; i++ {
		go mockSourceEgts(t, "localhost:7445", numOfRecs, uint32(numOfOids))
	}
	time.Sleep(2 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected = 1 + 1 + 1 + numEgtsSource + numEgtsSource*numOfRecs
	logrus.Println("start 3 test", expected, len(res))
	checkKeyNum(t, res, expected)

	time.Sleep(8 * time.Second)
	go mockEgtsServer(t, "localhost:7446")
	time.Sleep(15 * time.Second)
	res, err = getAllKeys(conn)
	if err != nil {
		t.Error(err)
	}
	expected = 1 + numOfEgtsServers
	logrus.Println("start 4 test", expected, len(res))
	checkKeyNum(t, res, expected)
}
