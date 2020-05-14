package server

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ashirko/navprot/pkg/ndtp"
	"github.com/ashirko/tcpmirror/internal/client"
	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/monitoring"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
)

type ndtpServer struct {
	conn       net.Conn
	terminalID int
	sessionID  int
	logger     *logrus.Entry
	pool       *db.Pool
	exitChan   chan struct{}
	*util.Options
	masterIn    chan []byte
	masterOut   chan []byte
	ndtpClients []client.Client
	channels    []chan []byte
	packetNum   uint32
	confChan    chan *db.ConfMsg
	name        string
}

func startNdtpServer(listen string, options *util.Options, channels []chan []byte, systems []util.System, confChan chan *db.ConfMsg) {
	pool := db.NewPool(options.DB)
	defer util.CloseAndLog(pool, logrus.WithFields(logrus.Fields{"main": "closing pool"}))
	l, err := net.Listen("tcp", listen)
	if err != nil {
		logrus.Fatalf("error while listening: %s", err)
		return
	}
	defer util.CloseAndLog(l, logrus.WithFields(logrus.Fields{"main": "closing listener"}))
	for {
		c, err := l.Accept()
		if err != nil {
			logrus.Errorf("error while accepting: %s", err)
		}
		logrus.Printf("accepted connection (%s <-> %s)", c.RemoteAddr(), c.LocalAddr())
		go initNdtpServer(c, pool, options, channels, systems, confChan)
	}
}

func initNdtpServer(c net.Conn, pool *db.Pool, options *util.Options, channels []chan []byte, systems []util.System,
	confChan chan *db.ConfMsg) {
	s, err := newNdtpServer(c, pool, options, channels, systems, confChan)
	if err != nil {
		logrus.Errorf("error during initialization new ndtp server: %s", err)
		return
	}
	s.logger.Tracef("newNdtpServer: %+v", s)
	err = s.waitFirstMessage()
	if err != nil {
		s.logger.Errorf("error getting new message: %s", err)
		return
	}
	s.startClients()
	go s.receiveFromMaster()
	go s.removeExpired()
	s.serverLoop()
}

func newNdtpServer(conn net.Conn, pool *db.Pool, options *util.Options, channels []chan []byte, systems []util.System,
	confChan chan *db.ConfMsg) (*ndtpServer, error) {
	exitChan := make(chan struct{})
	master, clients, err := initNdtpClients(systems, options, pool, exitChan, confChan)
	if err != nil {
		return nil, err
	}
	for _, c := range clients {
		channels = append(channels, c.InputChannel())
	}
	channels = append(channels, master.InputChannel())
	return &ndtpServer{
		conn:        conn,
		logger:      logrus.WithField("type", "ndtp_server"),
		pool:        pool,
		exitChan:    exitChan,
		Options:     options,
		masterIn:    master.InputChannel(),
		masterOut:   master.OutputChannel(),
		ndtpClients: append(clients, master),
		channels:    channels,
		name:        monitoring.TerminalName,
	}, nil
}

func (s *ndtpServer) receiveFromMaster() {
	for {
		select {
		case <-s.exitChan:
			return
		case packet := <-s.masterOut:
			monitoring.SendMetric(s.Options, s.name, monitoring.QueuedPkts, len(s.masterOut))
			s.logger.Tracef("received packet from master: %v", packet)
			err := s.send2terminal(packet)
			if err != nil {
				close(s.exitChan)
				return
			}
			monitoring.SendMetric(s.Options, s.name, monitoring.SentPkts, 1)
		}
	}
}

func (s *ndtpServer) serverLoop() {
	monitoring.NewConn(s.Options, s.name)
	var buf []byte
	var b [defaultBufferSize]byte
	for {
		s.logger.Debug("start reading from client")
		if err := s.conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			s.logger.Warningf("can't set read dead line: %s", err)
		}
		n, err := s.conn.Read(b[:])
		monitoring.SendMetric(s.Options, s.name, monitoring.RcvdBytes, n)
		s.logger.Debugf("received %d from client", n)
		util.PrintPacket(s.logger, "packet from client: ", b[:n])
		//todo remove after testing
		util.PrintPacketForDebugging(s.logger, "parsed packet from client:", b[:n])
		if err != nil {
			s.logger.Info("close ndtpServer: ", err)
			monitoring.DelConn(s.Options, s.name)
			close(s.exitChan)
			return
		}
		buf = append(buf, b[:n]...)
		s.logger.Debugf("len(buf) = %d", len(buf))
		var numPacks uint
		buf, numPacks = s.processBuf(buf)
		monitoring.SendMetric(s.Options, s.name, monitoring.RcvdPkts, numPacks)
	}
}

func (s *ndtpServer) processBuf(buf []byte) ([]byte, uint) {
	countPack := uint(0)
	for len(buf) > 0 {
		packet, rest, service, _, nphID, err := ndtp.SimpleParse(buf)
		s.logger.Tracef("service: %d, nphID: %d, packet: %v, err: %v", service, nphID, packet, err)
		s.logger.Tracef("len packet: %d, len buf: %d, service: %d", len(packet), len(rest), service)
		if err != nil {
			if len(rest) > defaultBufferSize {
				s.logger.Warningf("drop buffer: %s", err)
				return []byte(nil), countPack
			}
			return rest, countPack
		}
		buf = rest
		err = s.processPacket(packet, service)
		if err != nil {
			s.logger.Warningf("can't process message from client: %s", err)
			return []byte(nil), countPack
		}
		countPack++
	}
	return buf, countPack
}

func (s *ndtpServer) processPacket(packet []byte, service uint16) (err error) {
	data := util.Data{
		TerminalID: uint32(s.terminalID),
		SessionID:  uint16(s.sessionID),
		PacketNum:  s.packetNum,
		Packet:     packet,
	}
	s.packetNum++
	sdata := util.Serialize(data)
	if service != ndtp.NphSrvNavdata {
		s.send2Channel(s.masterIn, sdata)
	} else {
		err = db.Write2DB(s.pool, s.terminalID, sdata, s.logger)
		if err != nil {
			return
		}
		s.send2Channels(sdata)
		reply := ndtp.MakeReply(packet, ndtp.NphResultOk)
		err = s.send2terminal(reply)
	}
	return
}

func (s *ndtpServer) waitFirstMessage() error {
	var b [defaultBufferSize]byte
	if err := s.conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		s.logger.Warningf("can't set ReadDeadLine for client connection: %s", err)
	}
	n, err := s.conn.Read(b[:])
	monitoring.SendMetric(s.Options, s.name, monitoring.RcvdBytes, n)
	if err != nil {
		s.logger.Warningf("can't get first message from client: %s", err)
		return err
	}
	s.logger.Debugf("got first message from client %s", s.conn.RemoteAddr())
	util.PrintPacket(s.logger, "receive first packet: ", b[:n])
	return s.handleFirstMessage(b[:n])
}

func (s *ndtpServer) startClients() {
	s.logger.Tracef("start clients: %v", s.ndtpClients)
	for _, c := range s.ndtpClients {
		go client.Start(c)
	}
}

func (s *ndtpServer) handleFirstMessage(mes []byte) (err error) {
	packetData := new(ndtp.Packet)
	_, err = packetData.Parse(mes)
	if err != nil {
		err = fmt.Errorf("parse error: %s", err)
		return
	}
	id, err := packetData.GetID()
	if err != nil {
		err = fmt.Errorf("getID error: %s", err)
		return
	}
	s.terminalID = id
	if err = s.setSessionID(); err != nil {
		err = fmt.Errorf("setSessionID error: %s", err)
		return
	}
	monitoring.SendMetric(s.Options, s.name, monitoring.RcvdPkts, 1)
	s.setIDClients()
	ip := ip(s.conn)
	packetData.ChangeAddress(ip)
	util.PrintPacket(s.logger, "changed first packet: ", packetData.Packet)
	err = db.WriteConnDB(s.pool, s.terminalID, s.logger, packetData.Packet)
	if err != nil {
		err = fmt.Errorf("WriteConnDB error: %s", err)
		return
	}
	reply := packetData.Reply(ndtp.NphResultOk)
	err = s.send2terminal(reply)
	if err != nil {
		err = fmt.Errorf("send2terminal error: %s", err)
		return
	}
	return
}

func (s *ndtpServer) send2terminal(packet []byte) (err error) {
	util.PrintPacket(s.logger, "send to terminal:", packet)
	err = s.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	//todo remove after testing
	util.PrintPacketForDebugging(s.logger, "parsed packet to client:", packet)
	if err != nil {
		return
	}
	var n int
	n, err = s.conn.Write(packet)
	monitoring.SendMetric(s.Options, s.name, monitoring.SentBytes, n)
	return
}

func (s *ndtpServer) setIDClients() {
	for _, c := range s.ndtpClients {
		if ndtpClient, ok := c.(*client.Ndtp); ok {
			ndtpClient.SetID(s.terminalID)
		} else if masterClient, ok := c.(*client.NdtpMaster); ok {
			masterClient.SetID(s.terminalID)
		}
	}
}

func ip(c net.Conn) net.IP {
	ipPort := strings.Split(c.RemoteAddr().String(), ":")
	ip := ipPort[0]
	ip1 := net.ParseIP(ip)
	return ip1.To4()
}

func initNdtpClients(systems []util.System, options *util.Options, pool *db.Pool, exitChan chan struct{},
	confChan chan *db.ConfMsg) (master client.Client, clients []client.Client, err error) {
	for _, sys := range systems {
		if sys.IsMaster {
			if master != nil {
				err = errors.New("could be only one master system")
				return
			}
			logrus.Tracef("systems: %+v", sys)
			logrus.Tracef("options: %+v", sys)
			master = client.NewNdtpMaster(sys, options, pool, exitChan, confChan)
			logrus.Tracef("master: %+v", master)
		} else {
			switch sys.Protocol {
			case "NDTP":
				c := client.NewNdtp(sys, options, pool, exitChan, confChan)
				clients = append(clients, c)
			default:
				continue
			}
		}
	}
	return
}

func (s *ndtpServer) send2Channels(data []byte) {
	s.logger.Tracef("send2Channels %v : %v", s.channels, data)
	for _, channel := range s.channels {
		s.send2Channel(channel, data)
	}
}

func (s *ndtpServer) send2Channel(channel chan []byte, data []byte) {
	copyData := util.Copy(data)
	select {
	case channel <- copyData:
		return
	default:
		s.logger.Warningln("channel is full")
	}
}

func (s *ndtpServer) removeExpired() {
	tickerEx := time.NewTicker(1 * time.Hour)
	defer tickerEx.Stop()
	for {
		select {
		case <-s.exitChan:
			return
		case <-tickerEx.C:
			err := db.RemoveExpired(s.pool, s.terminalID, s.logger)
			if err != nil {
				s.logger.Errorf("can't remove expired data ndtp: %s", err)
			}
		}
	}
}

func (s *ndtpServer) setSessionID() (err error) {
	s.sessionID, err = db.NewSessionID(s.pool, s.terminalID, s.logger)
	return
}
