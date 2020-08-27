package client

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/monitoring"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/egorban/navprot/pkg/egts"
	"github.com/sirupsen/logrus"
)

// EgtsChanSize defines size of EGTS client input chanel buffer
const EgtsChanSize = 10000

// Egts describes EGTS client
type Egts struct {
	Input  chan []byte
	dbConn db.Conn
	*info
	*egtsSession
	*connection
	confChan chan *db.ConfMsg
}

type egtsSession struct {
	egtsMessageID uint16
	egtsRecID     uint16
	mu            sync.Mutex
}

// NewEgts creates new Egts client
func NewEgts(sys util.System, options *util.Options, confChan chan *db.ConfMsg) *Egts {
	c := new(Egts)
	c.info = new(info)
	c.egtsSession = new(egtsSession)
	c.connection = new(connection)
	c.id = sys.ID
	c.name = sys.Name
	c.address = sys.Address
	c.logger = logrus.WithFields(logrus.Fields{"type": "egts_client", "vis": sys.ID})
	c.Options = options
	c.Input = make(chan []byte, EgtsChanSize)
	c.confChan = confChan
	return c
}

// InputChannel implements method of Client interface
func (c *Egts) InputChannel() chan []byte {
	return c.Input
}

// OutputChannel implements method of Client interface
func (c *Egts) OutputChannel() chan []byte {
	return nil
}

func (c *Egts) start() {
	c.logger.Traceln("start")
	conn, err := net.Dial("tcp", c.address)
	if err != nil {
		c.logger.Errorf("error while connecting to EGTS server: %s", err)
		c.reconnect()
	} else {
		c.conn = conn
		c.open = true
	}
	switch c.ServerProtocol {
	case "NDTP":
		go c.old4Ndtp()
		go c.replyHandler()
		c.clientLoop4Ndtp()
	case "EGTS":
		go c.old4Egts()
		go c.replyHandler()
		c.clientLoop4Egts()
	default:
		logrus.Errorf("undefined server protocol: %s", c.ServerProtocol)
	}
}

func (c *Egts) send(buf []byte) (err error) {
	if c.open {
		util.PrintPacket(c.logger, "sending packet: ", buf)
		if err = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
			c.logger.Warningf("can't SetWriteDeadline: %s", err)
		}
		n, err := c.conn.Write(buf)
		if err != nil {
			c.conStatus()
		} else {
			monitoring.SendMetric(c.Options, c.name, monitoring.SentBytes, n)
		}
	}
	return err
}

func (c *Egts) replyHandler() {
	dbConn := db.Connect(c.DB)
	var buf []byte
	for {
		if c.open {
			buf = c.waitReply(dbConn, buf)
			c.logger.Tracef("replyRestBuf: %v", buf)
		} else {
			buf = []byte(nil)
			c.logger.Warningf("EGTS server closed")
			time.Sleep(time.Duration(TimeoutClose) * time.Second)
		}
	}
}

func (c *Egts) waitReply(dbConn db.Conn, restBuf []byte) []byte {
	var b [defaultBufferSize]byte
	if err := c.conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		c.logger.Warningf("can't SetReadDeadLine for egtsConn: %s", err)
	}
	n, err := c.conn.Read(b[:])
	if err != nil {
		c.logger.Warningf("can't get reply from c server %s", err)
		c.conStatus()
		time.Sleep(time.Duration(TimeoutErrorReply) * time.Second)
		return []byte(nil)
	}
	monitoring.SendMetric(c.Options, c.name, monitoring.RcvdBytes, n)
	util.PrintPacket(c.logger, "received packet: ", b[:n])
	c.logger.Tracef("packetLen: %d", n)
	restBuf = append(restBuf, b[:n]...)
	return c.handleReplyLoop(dbConn, restBuf)
}

func (c *Egts) handleReplyLoop(dbConn db.Conn, restBuf []byte) []byte {
	for len(restBuf) != 0 {
		packetData := new(egts.Packet)
		var err error
		restBuf, err = packetData.Parse(restBuf)
		if err != nil {
			c.logger.Errorf("error while parsing reply %v: %s", restBuf, err)
			return restBuf

		}
		err = c.handleReplies(dbConn, packetData)
		if err != nil {
			c.logger.Errorf("error while handling replies: %s", err)
			return restBuf
		}
	}
	return []byte(nil)
}

func (c *Egts) handleReplies(dbConn db.Conn, packetData *egts.Packet) (err error) {
	data, ok := packetData.Data.(*egts.Response)
	if !ok {
		return fmt.Errorf("expected reply packet but got: %v", packetData)
	}
	if data.ProcRes != 0 {
		c.logger.Warningf("reply with not ok status: %d; %v", data.ProcRes, packetData)
		return
	}
	for _, rec := range packetData.Records {
		for _, sub := range rec.Data {
			err = c.handleReply(dbConn, sub)
		}
	}
	return
}

func (c *Egts) handleReply(dbConn db.Conn, sub *egts.SubRecord) (err error) {
	conf, ok := sub.Data.(*egts.Confirmation)
	if sub.Type != egts.EgtsPtResponse || !ok {
		c.logger.Warningf("expected response subrecord but got %v", sub)
		return
	}
	if conf.RST != egts.Success {
		c.logger.Warningf("reply with not ok status: %v", conf)
	} else {
		err = c.handleSuccessReply(dbConn, conf.CRN)
	}
	return
}

func (c *Egts) handleSuccessReply(dbConn db.Conn, crn uint16) (err error) {
	err = db.ConfirmEgts(dbConn, crn, c.id, c.logger, c.confChan)
	return
}

func (c *Egts) conStatus() {
	logger := logrus.WithField("egts", "reconnect")
	logger.Println("start conStatus")
	c.mu.Lock()
	defer c.mu.Unlock()
	logger.Debugf("closed: %t; recon: %t", c.open, c.reconnecting)
	if !c.open || c.reconnecting {
		return
	}
	c.reconnecting = true
	if err := c.conn.Close(); err != nil {
		logger.Errorf("can't close egtsConn: %s", err)
	}
	c.open = false
	c.reconnect()
}

func (c *Egts) reconnect() {
	c.logger.Println("start reconnecting")
	for {
		for i := 0; i < 3; i++ {
			c.logger.Printf("try to reconnect: %d", i)
			cE, err := net.Dial("tcp", c.address)
			if err == nil {
				c.conn = cE
				c.open = true
				c.logger.Println("reconnected")
				go c.updateRecStatus()
				return
			}
			c.logger.Warningf("error while reconnecting to EGTS server: %s", err)
		}
		time.Sleep(time.Duration(TimeoutReconnect) * time.Second)
	}
}

func (c *Egts) updateRecStatus() {
	time.Sleep(1 * time.Minute)
	c.reconnecting = false
}

func (c *Egts) closeDBConn(conn db.Conn) {
	db.Close(conn)
}

func (c *Egts) getID(conn db.Conn) error {
	recID, err := db.GetEgtsID(conn, c.id)
	if err == nil {
		c.egtsRecID = recID
	}
	return err
}
