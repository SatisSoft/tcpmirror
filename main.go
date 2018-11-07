package main

import (
	"flag"
	"github.com/ashirko/go-metrics"
	"github.com/ashirko/go-metrics-graphite"
	"github.com/gomodule/redigo/redis"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
	"time"
)

const (
	defaultBufferSize = 1024
	headerSize        = 15
	writeTimeout      = 10 * time.Second
	readTimeout       = 180 * time.Second
)

var (
	listenAddress   string
	NDTPAddress     string
	EGTSAddress     string
	graphiteAddress string
	egtsConn        connection
	egtsCh          = make(chan rnisData, 10000)
	egtsMu          sync.Mutex
)

type connection struct {
	addr   string
	conn   net.Conn
	closed bool
	recon  bool
}

type session struct {
	clientNPLReqID uint16
	clientNPHReqID uint32
	serverNPLReqID uint16
	serverNPHReqID uint32
	muC            sync.Mutex
	muS            sync.Mutex
	id             int
	logger *logrus.Entry
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

func main() {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	flag.StringVar(&listenAddress, "l", "", "listen address (e.g. 'localhost:8080')")
	flag.StringVar(&NDTPAddress, "n", "", "send NDTP to address (e.g. 'localhost:8081')")
	flag.StringVar(&EGTSAddress, "e", "", "send EGTS to address (e.g. 'localhost:8082')")
	flag.StringVar(&graphiteAddress, "g", "", "graphite address (e.g. 'localhost:8083')")
	flag.Parse()
	if listenAddress == "" || NDTPAddress == "" || EGTSAddress == "" {
		flag.Usage()
		return
	}
	if graphiteAddress == "" {
		logrus.Println("don't send metrics to graphite")
	} else {
		startMetrics(graphiteAddress)
	}
	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		logrus.Fatalf("error while listening: %s", err)
	}
	egtsConn = connection{EGTSAddress, nil, true, false}
	cE, err := net.Dial("tcp", EGTSAddress)
	if err != nil {
		logrus.Errorf("error while connecting to EGTS server: %s", err)
	} else {
		egtsConn.conn = cE
		egtsConn.closed = false
	}
	egtsCr, err := redis.Dial("tcp", ":6379")
	defer egtsCr.Close()
	if err != nil {
		logrus.Errorf("error while connect to redis: %s\n", err)
		return
	}
	go egtsSession()
	go waitReplyEGTS()
	go egtsRemoveExpired()
	connNo := uint64(1)
	for {
		c, err := l.Accept()
		if err != nil {
			logrus.Errorf("error while accepting: %s", err)
		}
		logrus.Printf("accepted connection %d (%s <-> %s)", connNo, c.RemoteAddr(), c.LocalAddr())
		go handleConnection(c, connNo)
		connNo += 1
	}
}

func handleConnection(c net.Conn, connNo uint64) {
	logger := logrus.WithFields(logrus.Fields{"connNum" : connNo})
	ndtpConn := connection{NDTPAddress, nil, true, false}
	defer c.Close()
	cR, err := redis.Dial("tcp", ":6379")
	defer cR.Close()
	if err != nil {
		logger.Errorf("can't connect to redis: %s\n", err)
		return
	}
	var b [defaultBufferSize]byte
	c.SetReadDeadline(time.Now().Add(readTimeout))
	n, err := c.Read(b[:])
	if err != nil {
		logger.Warningf("can't get first message from client: %s", err)
		return
	}
	if enableMetrics {
		countClientNDTP.Inc(1)
	}
	logger.Debugf("got first message from client %s", c.RemoteAddr())
	firstMessage := b[:n]
	data, packet, _, err := parseNDTP(firstMessage)
	if err != nil {
		logger.Warningf("first message is incorrect: %s", err)
		return
	}
	if data.NPH.ServiceID != NPH_SRV_GENERIC_CONTROLS || data.NPH.NPHType != NPH_SGC_CONN_REQUEST {
		logger.Warningf("first message is not conn request. Service: %d, Type %d", data.NPH.ServiceID, data.NPH.NPHType)
	}
	ip := getIP(c)
	logger.Printf("ip: %s\n", ip)
	printPacket(logger,"before change first packet: ", packet)
	changeAddress(packet, ip)
	printPacket(logger,"after change first packet: ", packet)
	err = writeConnDB(cR, data.NPH.ID, packet)
	replyCopy := make([]byte, len(packet))
	copy(replyCopy, packet)
	var s session
	s.logger = logger
	if err != nil {
		errorReply(c, replyCopy, &s)
		return
	} else {
		reply(c, data.NPH, replyCopy, &s)
	}
	errClientCh := make(chan error)
	ErrNDTPCh := make(chan error)
	cN, err := net.Dial("tcp", NDTPAddress)
	s.id = int(data.NPH.ID)
	logger = logger.WithField("id", s.id)
	s.logger = logger
	go ndtpRemoveExpired(&s, ErrNDTPCh)
	var mu sync.Mutex
	if err != nil {
		logger.Warningf("can't connect to NDTP server: %s", err)
	} else {
		ndtpConn.conn = cN
		ndtpConn.closed = false
		sendFirstMessage(cR, &ndtpConn, &s, packet, ErrNDTPCh, &mu)
		cR.Close()
	}
	connect(c, &ndtpConn, ErrNDTPCh, errClientCh, &s, &mu)
FORLOOP:
	for {
		select {
		case err := <-errClientCh:
			logger.Printf("msg from errClientCh: %s", err)
			break FORLOOP
		}
	}
	close(ErrNDTPCh)
	logger.Printf("close connection to client")
	c.Close()
	if !ndtpConn.closed {
		logger.Printf("close connection to server")
		ndtpConn.conn.Close()
	}

}

func sendFirstMessage(cR redis.Conn, ndtpConn *connection, s *session, firstMessage []byte, ErrNDTPCh chan error, mu *sync.Mutex) {
	printPacket(s.logger,"sending first packet: ", firstMessage)
	ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := ndtpConn.conn.Write(firstMessage)
	if err != nil {
		ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
	}
}

func connect(origin net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, s *session, mu *sync.Mutex) {
	//connection with ATT
	go clientSession(origin, ndtpConn, ErrNDTPCh, errClientCh, s, mu)
	//connection with MGT
	go serverSession(origin, ndtpConn, ErrNDTPCh, errClientCh, s, mu)
}

func reply(c net.Conn, data nphData, packet []byte, s*session) error {
	if data.isResult {
		return nil
	} else {
		ans := answer(packet)
		c.SetWriteDeadline(time.Now().Add(writeTimeout))
		printPacket(s.logger,"reply: send answer: ", ans)
		_, err := c.Write(ans)
		return err
	}
}
func errorReply(c net.Conn, packet []byte, s *session) error {
	ans := errorAnswer(packet)
	c.SetWriteDeadline(time.Now().Add(writeTimeout))
	printPacket(s.logger,"errorReply: send error reply: ", ans)
	_, err := c.Write(ans)
	return err

}

func serverNPLID(s *session) uint16 {
	s.muS.Lock()
	nplID := s.serverNPLReqID
	s.serverNPLReqID++
	s.muS.Unlock()
	return nplID
}

func serverID(s *session) (uint16, uint32) {
	s.muS.Lock()
	nplID := s.serverNPLReqID
	nphID := s.serverNPHReqID
	s.serverNPLReqID++
	s.serverNPHReqID++
	s.muS.Unlock()
	return nplID, nphID
}
func clientID(s *session) (uint16, uint32) {
	s.muC.Lock()
	nplID := s.clientNPLReqID
	nphID := s.clientNPHReqID
	s.clientNPLReqID++
	s.clientNPHReqID++
	s.muC.Unlock()
	return nplID, nphID
}

func startMetrics(graphiteAddress string) {
	addr, err := net.ResolveTCPAddr("tcp", graphiteAddress)
	if err != nil {
		logrus.Errorf("error while connection to graphite: %s\n", err)
	} else {
		countClientNDTP = metrics.NewCustomCounter()
		countToServerNDTP = metrics.NewCustomCounter()
		countFromServerNDTP = metrics.NewCustomCounter()
		countServerEGTS = metrics.NewCustomCounter()
		memFree = metrics.NewGauge()
		memUsed = metrics.NewGauge()
		cpu15 = metrics.NewGaugeFloat64()
		cpu1 = metrics.NewGaugeFloat64()
		usedPercent = metrics.NewGaugeFloat64()
		metrics.Register("clNDTP", countClientNDTP)
		metrics.Register("toServNDTP", countToServerNDTP)
		metrics.Register("fromServNDTP", countFromServerNDTP)
		metrics.Register("servEGTS", countServerEGTS)
		metrics.Register("memFree", memFree)
		metrics.Register("memUsed", memUsed)
		metrics.Register("UsedPercent", usedPercent)
		metrics.Register("cpu15", cpu15)
		metrics.Register("cpu1", cpu1)
		enableMetrics = true
		logrus.Println("start sending metrics to graphite")
		go graphite.Graphite(metrics.DefaultRegistry, 10*10e8, "ndtpserv.metrics", addr)
		go periodicSysMon()
	}
}

func periodicSysMon() {
	for {
		v, err := mem.VirtualMemory()
		if err != nil {
			logrus.Errorf("periodic mem mon error: %s", err)
		} else {
			memFree.Update(int64(v.Free))
			memUsed.Update(int64(v.Used))
			usedPercent.Update(v.UsedPercent)
		}
		c, err := load.Avg()
		if err != nil {
			logrus.Errorf("periodic cpu mon error: %s", err)
		} else {
			cpu1.Update(c.Load1)
			cpu15.Update(c.Load15)
		}
		time.Sleep(10 * time.Second)
	}
}
