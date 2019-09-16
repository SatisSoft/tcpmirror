package server

import (
	"github.com/ashirko/navprot/pkg/egts"
	"github.com/ashirko/navprot/pkg/ndtp"
	"github.com/ashirko/tcpmirror/internal/client"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
	"net"
	"os"
	"testing"
	"time"
)

const (
	listen = "localhost:8800"
	ndtpMaster = "localhost:8801"
	ndtpClient = "localhost:8802"
	egtsClient = "localhost:8803"
)

func Test_startServer(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	systems := systems()
	args := args(systems)
	opts := options()
	egtsClients := startEgtsClients(opts, args)
	go mockNdtpMasterClient(t)
	go mockNdtpClient(t)
	go mockEgtsClient(t)
	go mockTerminal(t)
	startServer(args, opts, egtsClients)
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

func args(systems []util.System) *util.Args {
	return &util.Args{
		Listen:     listen,
		Protocol:   "NDTP",
		Systems:    systems,
		Monitoring: "",
		DB:         "localhost:9999",
	}
}

func systems() []util.System {
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
var packetNav = []byte{0, 80, 86, 161, 44, 216, 192, 140, 96, 196, 138, 54, 8, 0, 69, 0, 0, 129, 102, 160, 64, 0, 125, 6,
	18, 51, 10, 68, 41, 150, 10, 176, 70, 26, 236, 153, 35, 56, 151, 147, 73, 96, 98, 94, 76, 40, 80,
	24, 1, 2, 190, 27, 0, 0, 126, 126, 74, 0, 2, 0, 107, 210, 2, 0, 0, 0, 0, 0, 0, 1, 0, 101, 0, 1, 0, 171,
	20, 0, 0, 0, 0, 36, 141, 198, 90, 87, 110, 119, 22, 201, 186, 64, 33, 224, 203, 0, 0, 0, 0, 83, 1, 0,
	0, 220, 0, 4, 0, 2, 0, 22, 0, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 167, 97, 0, 0, 31, 6, 0, 0, 8,
	0, 2, 0, 0, 0, 0, 0, 1, 2, 3}

func mockTerminal(t *testing.T) {
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", listen)
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()
	err = send(conn, packetAuth)
	if err != nil {
		t.Error(err)
	}
	var b [defaultBufferSize]byte
	_, err = conn.Read(b[:])
	if err != nil {
		t.Error(err)
	}
	time.Sleep(100 * time.Millisecond)
	err = send(conn, packetNav)
	if err != nil {
		t.Error(err)
	}
	_, err = conn.Read(b[:])
	if err != nil {
		t.Error(err)
	}
	time.Sleep(10 *time.Second)
}

func mockNdtpMasterClient(t *testing.T) {
	l, err := net.Listen("tcp", ndtpMaster)
	if err != nil {
		t.Error(err)
	}
	c, err := l.Accept()
	if err != nil {
		t.Error(err)
	}
	var b [defaultBufferSize]byte
	n, err := c.Read(b[:])
	if err != nil {
		t.Error(err)
	}
	p := new(ndtp.Packet)
	_, err = p.Parse(b[:n])
	logrus.Println("master received:", p.Packet)
	if err != nil {
		t.Error(err)
	}
	rep := p.Reply(0)
	logrus.Println("master send:", rep)
	err = send(c, rep)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(100 * time.Millisecond)
}

func mockNdtpClient(t *testing.T) {
	l, err := net.Listen("tcp", ndtpClient)
	if err != nil {
		t.Error(err)
	}
	c, err := l.Accept()
	if err != nil {
		t.Error(err)
	}
	var b [defaultBufferSize]byte
	n, err := c.Read(b[:])
	if err != nil {
		t.Error(err)
	}
	p := new(ndtp.Packet)
	_, err = p.Parse(b[:n])
	logrus.Println("ndtp client received:", p.Packet)
	if err != nil {
		t.Error(err)
	}
	rep := p.Reply(0)
	logrus.Println("ndtp client send:", rep)
	err = send(c, rep)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(100 * time.Millisecond)
}

func mockEgtsClient(t *testing.T) {
	l, err := net.Listen("tcp", egtsClient)
	if err != nil {
		t.Error(err)
	}
	c, err := l.Accept()
	if err != nil {
		t.Error(err)
	}
	var b [defaultBufferSize]byte
	n, err := c.Read(b[:])
	if err != nil {
		t.Error(err)
	}
	p := new(egts.Packet)
	_, err = p.Parse(b[:n])
	logrus.Println("egts client received:", p)
	if err != nil {
		t.Error(err)
	}
	//rep := p.Reply(0)
	//logrus.Println("egts reply:", rep)
	//err = send(c, rep)
	//if err != nil {
	//	t.Error(err)
	//}
	time.Sleep(100 * time.Millisecond)
}

func send(conn net.Conn, packet []byte) error {
	err := conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err != nil {
		return err
	}
	_, err = conn.Write(packet)
	return err
}