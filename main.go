package main

import (
	"flag"
	"github.com/ashirko/go-metrics"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"net"
	"strings"
	"sync"
	"time"
)

import nav "github.com/ashirko/navprot"

const (
	defaultBufferSize = 1024
	writeTimeout      = 10 * time.Second
	readTimeout       = 180 * time.Second
)

const nphResultOk uint32 = 0
const nphResultError uint32 = 103

var (
	listenAddress   string
	ndtpServer      string
	egtsServer      string
	graphiteAddress string
	redisServer     string
	egtsConn        connection
	egtsCh          = make(chan *egtsMsg, 10000)
	egtsMu          sync.Mutex
	egtsKey         string
	egtsIDKey       string
	pool            *redis.Pool
)

type connection struct {
	addr   string
	conn   net.Conn
	closed bool
	recon  bool
}

type egtsMsg struct {
	msgID string
	msg   *nav.EGTS
}

var (
	countClientNDTP     metrics.Counter
	countToServerNDTP   metrics.Counter
	countFromServerNDTP metrics.Counter
	countServerEGTS     metrics.Counter
	memFree             metrics.Gauge
	memUsed             metrics.Gauge
	cpu15               metrics.GaugeFloat64
	cpu1                metrics.GaugeFloat64
	usedPercent         metrics.GaugeFloat64
	enableMetrics       bool
)

func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     200,
		MaxActive:   0,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

func main() {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	flag.StringVar(&listenAddress, "l", "", "listen address (e.g. 'localhost:8080')")
	flag.StringVar(&ndtpServer, "n", "", "send NDTP to address (e.g. 'localhost:8081')")
	flag.StringVar(&egtsServer, "e", "", "send EGTS to address (e.g. 'localhost:8082')")
	flag.StringVar(&graphiteAddress, "g", "", "graphite address (e.g. 'localhost:8083')")
	flag.StringVar(&redisServer, "r", ":6379", "redis server address (e.g. 'localhost:6379')")
	flag.Parse()
	if listenAddress == "" || ndtpServer == "" || egtsServer == "" {
		flag.Usage()
		return
	}
	listenPort := strings.Split(listenAddress, ":")
	egtsIDKey = "egts_" + listenPort[1] + "_"
	egtsKey = "rnis_" + listenPort[1]
	logrus.Infof("egtsIDKey: %s; egtsKey : %s", egtsIDKey, egtsKey)
	pool = newPool(redisServer)
	defer closeAndLog(pool, logrus.WithFields(logrus.Fields{"main": "closing pool"}))
	if graphiteAddress == "" {
		logrus.Println("don't send metrics to graphite")
	} else {
		startMetrics(graphiteAddress)
	}
	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		logrus.Fatalf("error while listening: %s", err)
	}
	go egtsServerSession()
	connNo := uint64(1)
	for {
		c, err := l.Accept()
		if err != nil {
			logrus.Errorf("error while accepting: %s", err)
		}
		logrus.Printf("accepted connection %d (%s <-> %s)", connNo, c.RemoteAddr(), c.LocalAddr())
		go handleConnection(c, connNo)
		connNo++
	}
}

func handleConnection(c net.Conn, connNo uint64) {
	s, err := newSession(c, connNo)
	if err != nil {
		s.logger.Warningf("can't create session: %v", err)
	}
	defer closeSession(s)
	waitFirstMessage(c, s)
	connect(s)
	err = <-s.errClientCh
	s.logger.Debugf("msg from errClientCh: %s", err)
	return
}

func waitFirstMessage(c net.Conn, s *session) {
	var b [defaultBufferSize]byte
	if err := c.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		s.logger.Warningf("can't set ReadDeadLine for client connection: %s", err)
	}
	n, err := c.Read(b[:])
	if err != nil {
		s.logger.Warningf("can't get first message from client: %s", err)
		return
	}
	if enableMetrics {
		countClientNDTP.Inc(1)
	}
	s.logger.Debugf("got first message from client %s", c.RemoteAddr())
	printPacket(s.logger, "receive first packet: ", b[:n])
	handleFirstMessage(c, s, b[:n])
}

func handleFirstMessage(c net.Conn, s *session, mes []byte) {
	ndtp := new(nav.NDTP)
	_, err := ndtp.Parse(mes)
	if err != nil {
		s.logger.Warningf("first message is incorrect: %s", err)
		return
	}
	id, err := ndtp.GetID()
	if err != nil {
		s.logger.Warningf("can't get id from first message: %v", err)
	}
	s.setID(id)
	ip := ip(c)
	ndtp.ChangeAddress(ip)
	printPacket(s.logger, "changed first packet: ", ndtp.Packet)
	err = writeConnDB(s, ndtp.Packet)
	if err != nil {
		s.logger.Errorf("writeConnDB error: %v", err)
		err = reply(s, ndtp, nphResultError)
		if err != nil {
			s.logger.Errorf("error sending reply to client: %s", err)
		}
		return
	}
	err = reply(s, ndtp, nphResultOk)
	if err != nil {
		s.logger.Errorf("error sending reply to client: %s", err)
	}
	s.servConn.conn, err = net.Dial("tcp", ndtpServer)
	if err != nil {
		s.logger.Warningf("can't connect to NDTP server: %s", err)
	} else {
		s.servConn.closed = false
		sendFirstMessage(s, ndtp)
	}
}

func sendFirstMessage(s *session, ndtp *nav.NDTP) {
	printPacket(s.logger, "sending first packet: ", ndtp.Packet)
	err := sendToServer(s, ndtp)
	if err != nil {
		s.logger.Warningf("can't send first packet: %v", err)
		ndtpConStatus(s)
	}
}

func connect(s *session) {
	//connection with ATT
	go clientSession(s)
	//connection with MGT
	go serverSession(s)
}
