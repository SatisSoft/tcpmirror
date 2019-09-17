package server

import (
	"fmt"
	"github.com/ashirko/navprot/pkg/ndtp"
	"github.com/ashirko/tcpmirror/internal/client"
	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/egorban/egtsServ/pkg/egtsserv"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"net"
	"os"
	"testing"
	"time"
)

const (
	listen     = "localhost:8800"
	ndtpMaster = "localhost:8901"
	ndtpClient = "localhost:8902"
	egtsClient = "localhost:8903"
)

func Test_startServerOneClient(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	systems := systemsOne()
	args := argsOne(systems)
	opts := options()
	con := db.Connect(opts.DB)
	_, err := con.Do("FLUSHALL")
	if err != nil {
		t.Error(err)
	}
	defer con.Close()
	db.SysNumber = len(args.Systems)
	go mockNdtpMasterClient(t)
	go mockTerminal(t, args.Listen)
	go startServer(args, opts, nil)
	time.Sleep(25 * time.Second)
	res, err := redis.ByteSlices(con.Do("KEYS", "*"))
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("res: %v; err: %v\n", res, err)
	if len(res) != 3 {
		t.Errorf("expected 2 keys in DB. Got %d: %v", len(res), res)
	}
}

func Test_startServerOneClientDouble(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	systems := systemsOne()
	args := argsDouble(systems)
	opts := options()
	con := db.Connect(opts.DB)
	_, err := con.Do("FLUSHALL")
	if err != nil {
		t.Error(err)
	}
	defer con.Close()
	db.SysNumber = len(args.Systems)
	go mockNdtpMasterClient(t)
	go mockTerminalDouble(t, args.Listen)
	go startServer(args, opts, nil)
	time.Sleep(25 * time.Second)
	res, err := redis.ByteSlices(con.Do("KEYS", "*"))
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("res: %v; err: %v\n", res, err)
	if len(res) != 3 {
		t.Errorf("expected 2 keys in DB. Got %d: %v", len(res), res)
	}
}

func Test_startServerTwoClients(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	systems := systemsTwo()
	args := argsTwo(systems)
	opts := options()
	con := db.Connect(opts.DB)
	_, err := con.Do("FLUSHALL")
	if err != nil {
		t.Error(err)
	}
	defer con.Close()
	db.SysNumber = len(args.Systems)
	go mockNdtpMasterClient(t)
	go mockNdtpClient(t)
	go mockTerminal(t, args.Listen)
	go startServer(args, opts, nil)
	time.Sleep(25 * time.Second)
	res, err := redis.ByteSlices(con.Do("KEYS", "*"))
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("res: %v; err: %v\n", res, err)
	if len(res) != 4 {
		t.Errorf("expected 2 keys in DB. Got %d: %v", len(res), res)
	}
}

func Test_startServerThreeClients(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	systems := systemsThree()
	args := argsThree(systems)
	opts := options()
	con := db.Connect(opts.DB)
	_, err := con.Do("FLUSHALL")
	if err != nil {
		t.Error(err)
	}
	db.SysNumber = len(args.Systems)
	egtsClients := startEgtsClients(opts, args)
	go mockNdtpMasterClient(t)
	go mockNdtpClient(t)
	go mockEgtsClient(t, egtsClient)
	go mockTerminal(t, args.Listen)
	go startServer(args, opts, egtsClients)
	time.Sleep(25 * time.Second)
	res, err := redis.ByteSlices(con.Do("KEYS", "*"))
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("res: %v; err: %v\n", res, err)
	if len(res) != 5 {
		t.Errorf("expected 5 keys in DB. Got %d: %v", len(res), res)
	}
}

func Test_startServerOneClientExt(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	systems := systemsOne()
	args := argsOne(systems)
	opts := options()
	con := db.Connect(opts.DB)
	_, err := con.Do("FLUSHALL")
	if err != nil {
		t.Error(err)
	}
	defer con.Close()
	db.SysNumber = len(args.Systems)
	go mockNdtpMasterClientExt(t)
	go mockTerminalExt(t, args.Listen)
	go startServer(args, opts, nil)
	time.Sleep(25 * time.Second)
	res, err := redis.ByteSlices(con.Do("KEYS", "*"))
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("res: %v; err: %v\n", res, err)
	if len(res) != 3 {
		t.Errorf("expected 2 keys in DB. Got %d: %v", len(res), res)
	}
}

func startEgtsClients(options *util.Options, args *util.Args) []client.Client {
	egtsClients, err := initEgtsClients(options, args)
	logrus.Tracef("newEgtsClients: %v", egtsClients)
	if err != nil {
		logrus.Error("can't init clients:", err)
		os.Exit(1)
	}
	startClients(egtsClients)
	return egtsClients
}

func argsOne(systems []util.System) *util.Args {
	return &util.Args{
		Listen:     "localhost:8800",
		Protocol:   "NDTP",
		Systems:    systems,
		Monitoring: "",
		DB:         "localhost:9999",
	}
}

func argsTwo(systems []util.System) *util.Args {
	return &util.Args{
		Listen:     "localhost:8801",
		Protocol:   "NDTP",
		Systems:    systems,
		Monitoring: "",
		DB:         "localhost:9999",
	}
}

func argsDouble(systems []util.System) *util.Args {
	return &util.Args{
		Listen:     "localhost:8802",
		Protocol:   "NDTP",
		Systems:    systems,
		Monitoring: "",
		DB:         "localhost:9999",
	}
}

func argsThree(systems []util.System) *util.Args {
	return &util.Args{
		Listen:     "localhost:8803",
		Protocol:   "NDTP",
		Systems:    systems,
		Monitoring: "",
		DB:         "localhost:9999",
	}
}

func systemsOne() []util.System {
	return []util.System{
		{
			ID:       1,
			Address:  ndtpMaster,
			Protocol: "NDTP",
			IsMaster: true,
		},
	}
}

func systemsTwo() []util.System {
	return []util.System{
		{
			ID:       1,
			Address:  ndtpMaster,
			Protocol: "NDTP",
			IsMaster: true,
		},
		{
			ID:       2,
			Address:  ndtpClient,
			Protocol: "NDTP",
			IsMaster: false,
		},
	}
}

func systemsThree() []util.System {
	return []util.System{
		{
			ID:       1,
			Address:  ndtpMaster,
			Protocol: "NDTP",
			IsMaster: true,
		},
		{
			ID:       2,
			Address:  ndtpClient,
			Protocol: "NDTP",
			IsMaster: false,
		},
		{
			ID:       3,
			Address:  egtsClient,
			Protocol: "EGTS",
			IsMaster: false,
		},
	}
}

func options() *util.Options {
	return &util.Options{
		Mon: false,
		DB:  "localhost:9999",
	}
}

var packetAuth = []byte{1, 2, 3, 126, 126, 59, 0, 2, 0, 14, 84, 2, 0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 1, 0, 0, 0, 0, 0, 6, 0, 2, 0, 2, 3, 90, 139, 1, 0, 0, 4, 0, 0, 0, 0, 0, 0, 51, 53, 53, 48, 57, 52, 48, 52, 51, 49, 56, 56, 51, 49, 49, 50, 53, 48, 48, 49, 54, 53, 48, 53, 56, 49, 53, 53, 51, 55, 0, 1, 2, 3}

var packetNav = []byte{126, 126, 74, 0, 2, 0, 107, 210, 2, 0, 0, 0, 0, 0, 0, 1, 0, 101, 0, 1, 0, 171,
	20, 0, 0, 0, 0, 36, 141, 198, 90, 87, 110, 119, 22, 201, 186, 64, 33, 224, 203, 0, 0, 0, 0, 83, 1, 0,
	0, 220, 0, 4, 0, 2, 0, 22, 0, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 167, 97, 0, 0, 31, 6, 0, 0, 8,
	0, 2, 0, 0, 0, 0, 0}

var packetExt = []byte{126, 126, 90, 1, 2, 0, 33, 134, 2, 0, 4, 0, 0, 144, 7, 5, 0, 100, 0, 0, 0, 1, 0, 0, 0, 18, 0, 0, 128, 0, 0, 0, 0, 1, 0, 0, 0, 60, 78, 65, 86, 83, 67, 82, 32, 118, 101, 114, 61, 49, 46, 48, 62, 60, 73, 68, 62, 49, 56, 60, 47, 73, 68, 62, 60, 70, 82, 79, 77, 62, 83, 69, 82, 86, 69, 82, 60, 47, 70, 82, 79, 77, 62, 60, 84, 79, 62, 85, 83, 69, 82, 60, 47, 84, 79, 62, 60, 84, 89, 80, 69, 62, 81, 85, 69, 82, 89, 60, 47, 84, 89, 80, 69, 62, 60, 77, 83, 71, 32, 116, 105, 109, 101, 61, 54, 48, 32, 98, 101, 101, 112, 61, 49, 32, 116, 121, 112, 101, 61, 98, 97, 99, 107, 103, 114, 111, 117, 110, 100, 62, 60, 98, 114, 47, 62, 60, 98, 114, 47, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 194, 251, 32, 236, 229, 237, 255, 32, 241, 235, 251, 248, 232, 242, 229, 63, 60, 98, 114, 47, 62, 60, 98, 114, 47, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 60, 98, 116, 110, 49, 62, 196, 224, 60, 47, 98, 116, 110, 49, 62, 60, 98, 114, 47, 62, 60, 98, 114, 47, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 60, 98, 116, 110, 50, 62, 205, 229, 242, 60, 47, 98, 116, 110, 50, 62, 60, 98, 114, 47, 62, 60, 47, 77, 83, 71, 62, 60, 47, 78, 65, 86, 83, 67, 82, 62}

func mockTerminal(t *testing.T, addr string) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_terminal"})
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()
	sendAndReceive(t, conn, packetAuth, logger)
	time.Sleep(100 * time.Millisecond)
	nphID := 5291
	sendAndReceive(t, conn, packetNav, logger)
	logger.Tracef("packNav1: %v", packetNav)
	nphID++
	changes := map[string]int{ndtp.NphReqID: nphID}
	packetNav = ndtp.Change(packetNav, changes)
	logger.Tracef("packNav2: %v", packetNav)
	sendAndReceive(t, conn, packetNav, logger)
	time.Sleep(10 * time.Second)
}

func mockTerminalDouble(t *testing.T, addr string) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_terminal_double"})
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()
	sendAndReceive(t, conn, packetAuth, logger)
	time.Sleep(100 * time.Millisecond)
	sendAndReceive(t, conn, packetNav, logger)
	logger.Tracef("packNav1: %v", packetNav)
	logger.Tracef("packNav2: %v", packetNav)
	sendAndReceive(t, conn, packetNav, logger)
	time.Sleep(10 * time.Second)
}

func sendAndReceive(t *testing.T, c net.Conn, packet []byte, logger *logrus.Entry) {
	err := send(c, packet)
	logger.Tracef("send: %v", packet)
	if err != nil {
		logger.Errorf("got error: %v", err)
		t.Error(err)
	}
	var b [defaultBufferSize]byte
	n, err := c.Read(b[:])
	logger.Tracef("receive: %v", b[:n])
	if err != nil {
		logger.Errorf("got error: %v", err)
		t.Error(err)
	}
}

func mockNdtpMasterClient(t *testing.T) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_master"})
	l, err := net.Listen("tcp", ndtpMaster)
	if err != nil {
		t.Error(err)
	}
	defer l.Close()
	c, err := l.Accept()
	if err != nil {
		t.Error(err)
	}
	defer c.Close()
	// first
	receiveAndReply(t, c, logger)
	// second
	receiveAndReply(t, c, logger)
	// third
	receiveAndReply(t, c, logger)
	time.Sleep(1000 * time.Millisecond)
}

func mockTerminalExt(t *testing.T, addr string) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_terminal"})
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()
	sendAndReceive(t, conn, packetAuth, logger)
	time.Sleep(100 * time.Millisecond)
	nphID := 5291
	sendAndReceive(t, conn, packetNav, logger)
	logger.Tracef("packNav1: %v", packetNav)
	nphID++
	changes := map[string]int{ndtp.NphReqID: nphID}
	packetNav = ndtp.Change(packetNav, changes)
	logger.Tracef("packNav2: %v", packetNav)
	sendAndReceive(t, conn, packetNav, logger)
	// ext
	receiveAndReply(t, conn, logger)
	time.Sleep(10 * time.Second)
}

func mockNdtpMasterClientExt(t *testing.T) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_master"})
	l, err := net.Listen("tcp", ndtpMaster)
	if err != nil {
		t.Error(err)
	}
	defer l.Close()
	c, err := l.Accept()
	if err != nil {
		t.Error(err)
	}
	defer c.Close()
	// first
	receiveAndReply(t, c, logger)
	// second
	receiveAndReply(t, c, logger)
	// third
	receiveAndReply(t, c, logger)
	sendAndReceive(t, c, packetExt, logger)
	time.Sleep(1000 * time.Millisecond)
}

func receiveAndReply(t *testing.T, c net.Conn, logger *logrus.Entry) {
	var b [defaultBufferSize]byte
	n, err := c.Read(b[:])
	if err != nil {
		logger.Errorf("got error: %v", err)
		t.Error(err)
	}
	p := new(ndtp.Packet)
	_, err = p.Parse(b[:n])
	logger.Println("received:", p.Packet)
	if err != nil {
		logger.Errorf("got error: %v", err)
		t.Error(err)
	}
	rep := p.Reply(0)
	logger.Println("send:", rep)
	err = send(c, rep)
	if err != nil {
		logger.Errorf("got error: %v", err)
		t.Error(err)
	}
}

func mockNdtpClient(t *testing.T) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_ndtp"})
	l, err := net.Listen("tcp", ndtpClient)
	if err != nil {
		t.Error(err)
	}
	defer l.Close()
	c, err := l.Accept()
	if err != nil {
		logger.Errorf("got error: %v", err)
		t.Error(err)
	}
	defer c.Close()
	// first
	receiveAndReply(t, c, logger)
	// second
	receiveAndReply(t, c, logger)
	// third
	receiveAndReply(t, c, logger)
	time.Sleep(1000 * time.Millisecond)
}

func mockEgtsClient(t *testing.T, addr string) {
	egtsserv.Start(addr)
}

func send(conn net.Conn, packet []byte) error {
	err := conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err != nil {
		return err
	}
	_, err = conn.Write(packet)
	return err
}
