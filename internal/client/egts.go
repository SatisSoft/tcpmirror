package client

import (
	"fmt"
	"github.com/ashirko/navprot/pkg/egts"
	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
	"time"
)

// EgtsChanSize defines size of EGTS client input chanel buffer
const EgtsChanSize = 10000

// Egts describes EGTS client
type Egts struct {
	Input  chan []byte
	dbConn db.Conn
	*info
	*egtsSession
	//dbConn db.Conn
	*connection
}

type egtsSession struct {
	egtsMessageID uint16
	egtsRecID     uint16
	mu            sync.Mutex
}

// NewEgts creates new Egts client
func NewEgts(sys util.System, options *util.Options) *Egts {
	c := new(Egts)
	c.info = new(info)
	c.egtsSession = new(egtsSession)
	c.connection = new(connection)
	c.id = sys.ID
	c.address = sys.Address
	c.logger = logrus.WithFields(logrus.Fields{"type": "egts_client", "vis": sys.ID})
	c.Options = options
	c.Input = make(chan []byte, EgtsChanSize)
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
	} else {
		c.conn = conn
		c.open = true
	}
	//todo handle reconnection
	go c.old()
	go c.replyHandler()
	c.clientLoop()
}

func (c *Egts) stop() error {
	//todo close connection to DB, tcp connection to EGTS server
	return nil
}

func (c *Egts) clientLoop() {
	dbConn := db.Connect(c.DB)
	defer c.closeDBConn(dbConn)
	err := c.getID(dbConn)
	if err != nil {
		// todo monitor this error
		c.logger.Errorf("can't getID: %v", err)
	}
	var buf []byte
	count := 0
	sendTicker := time.NewTicker(100 * time.Millisecond)
	defer sendTicker.Stop()
	for {
		select {
		case message := <-c.Input:
			buf = c.processMessage(dbConn, message, buf)
			count++
			if count == 10 {
				c.send(buf)
				buf = []byte(nil)
				count = 0
			}
		case <-sendTicker.C:
			if (count > 0) && (count < 10) {
				c.send(buf)
				buf = []byte(nil)
				count = 0
			}
		}
	}
}

func (c *Egts) processMessage(dbConn db.Conn, message []byte, buf []byte) []byte {
	util.PrintPacket(c.logger, "serialized data: ", message)
	data := util.Deserialize(message)
	c.logger.Tracef("data: %+v", data)
	messageID, recID, err := c.ids(dbConn)
	if err != nil {
		c.logger.Errorf("can't get ids: %s", err)
		return buf
	}
	packet, err := util.Ndtp2Egts(data.Packet, data.TerminalID, messageID, recID)
	util.PrintPacket(c.logger, "formed packet: ", packet)
	if err != nil {
		c.logger.Errorf("can't form packet: %s", err)
		return buf
	}
	buf = append(buf, packet...)
	err = db.WriteEgtsID(dbConn, c.id, recID, data.ID)
	if err != nil {
		c.logger.Errorf("error WriteEgtsID: %s", err)
	}
	return buf
}

func (c *Egts) ids(conn db.Conn) (uint16, uint16, error) {
	c.mu.Lock()
	egtsMessageID := c.egtsMessageID
	egtsRecID := c.egtsRecID
	c.egtsMessageID++
	c.egtsRecID++
	err := db.SetEgtsID(conn, c.id, c.egtsRecID)
	c.mu.Unlock()
	return egtsMessageID, egtsRecID, err
}

func (c *Egts) send(buf []byte) {
	if c.open {
		util.PrintPacket(c.logger, "sending packet: ", buf)
		if err := c.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
			c.logger.Warningf("can't SetWriteDeadline: %s", err)
		}
		_, err := c.conn.Write(buf)
		if err != nil {
			c.conStatus()
		}
	}
}

func (c *Egts) replyHandler() {
	dbConn := db.Connect(c.DB)
	for {
		if c.open {
			c.waitReply(dbConn)
		} else {
			c.logger.Warningf("EGTS server closed")
			time.Sleep(5 * time.Second)
		}
	}
}

func (c *Egts) waitReply(dbConn db.Conn) {
	var b [defaultBufferSize]byte
	var restBuf []byte
	if err := c.conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		c.logger.Warningf("can't SetReadDeadLine for egtsConn: %s", err)
	}
	n, err := c.conn.Read(b[:])
	util.PrintPacket(c.logger, "received packet: ", b[:n])
	if err != nil {
		c.logger.Warningf("can't get reply from c server %s", err)
		c.conStatus()
		time.Sleep(5 * time.Second)
		return
	}
	restBuf = append(restBuf, b[:n]...)
	c.handleReplyLoop(dbConn, restBuf)
}

func (c *Egts) handleReplyLoop(dbConn db.Conn, restBuf []byte) {
	for {
		packetData := new(egts.Packet)
		var err error
		restBuf, err = packetData.Parse(restBuf)
		if err != nil {
			c.logger.Errorf("error while parsing reply %v: %s", restBuf, err)
			return

		}
		err = c.handleReplies(dbConn, packetData)
		if err != nil {
			c.logger.Errorf("error while handling replies: %s", err)
		}
		if len(restBuf) == 0 {
			return
		}
	}
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
	// todo update navprot dep
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
	err = db.ConfirmEgts(dbConn, crn, c.id)
	return
}

func (c *Egts) old() {
	dbConn := db.Connect(c.DB)
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		c.logger.Debugf("start checking old data")
		messages, err := db.OldPacketsEGTS(dbConn, c.id)
		if err != nil {
			c.logger.Warningf("can't get old packets: %s", err)
			return
		}
		var buf []byte
		var i int
		for _, msg := range messages {
			if i < 10 {
				buf = c.processMessage(dbConn, msg, buf)
				i++
			} else {
				c.logger.Debugf("send old EGTS packets to EGTS server: %v", buf)
				c.send(buf)
				i = 0
				buf = []byte(nil)
			}
		}
		if len(buf) > 0 {
			c.logger.Debugf("oldEGTS: send rest packets to EGTS server: %v", buf)
			c.send(buf)
		}

	}
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
				c.logger.Printf("reconnected")
				go c.updateRecStatus()
				return
			}
			c.logger.Warningf("error while reconnecting to EGTS server: %s", err)
		}
		time.Sleep(10 * time.Second)
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
