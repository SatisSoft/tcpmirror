package client

import (
	"errors"
	"github.com/ashirko/navprot/pkg/ndtp"
	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
	"net"
	"time"
)

// NdtpMasterChanSize defines size of NdtpMaster client input chanel buffer
const NdtpMasterChanSize = 200

// NdtpMaster describes  Ndtp master client
type NdtpMaster struct {
	Input      chan []byte
	Output     chan []byte
	auth       bool
	exitChan   chan struct{}
	pool       *db.Pool
	terminalID int
	*info
	*ndtpSession
	*connection
}

// NewNdtpMaster creates new NdtpMaster client
func NewNdtpMaster(sys util.System, options *util.Options, pool *db.Pool, exitChan chan struct{}) *NdtpMaster {
	c := new(NdtpMaster)
	c.info = new(info)
	c.ndtpSession = new(ndtpSession)
	c.connection = new(connection)
	c.id = sys.ID
	c.address = sys.Address
	c.logger = logrus.WithFields(logrus.Fields{"type": "ndtp_master_client", "vis": sys.ID})
	c.Options = options
	c.Input = make(chan []byte, NdtpMasterChanSize)
	c.Output = make(chan []byte, NdtpMasterChanSize)
	c.exitChan = exitChan
	c.pool = pool
	return c
}

func (c *NdtpMaster) start() {
	err := c.setNph()
	if err != nil {
		// todo monitor this error
		c.logger.Errorf("can't setNph: %v", err)
	}
	c.logger.Traceln("start")
	conn, err := net.Dial("tcp", c.address)
	if err != nil {
		c.logger.Errorf("error while connecting to NDTP master server %d: %s", c.id, err)
		c.reconnect()
	} else {
		c.conn = conn
		c.open = true
	}
	if c.serverClosed() {
		return
	}
	go c.old()
	go c.replyHandler()
	c.clientLoop()
}

func (c *NdtpMaster) stop() error {
	//todo close connection to DB, tcp connection to EGTS server
	return nil
}

// InputChannel implements method of Client interface
func (c *NdtpMaster) InputChannel() chan []byte {
	return c.Input
}

// OutputChannel implements method of Client interface
func (c *NdtpMaster) OutputChannel() chan []byte {
	return c.Output
}

// SetID sets terminalID
func (c *NdtpMaster) SetID(terminalID int) {
	c.terminalID = terminalID
}

func (c *NdtpMaster) clientLoop() {
	err := c.sendFirstMessage()
	if err != nil {
		c.logger.Errorf("can't send first message: %s", err)
		c.connStatus()
	}
	for {
		select {
		case <-c.exitChan:
			return
		case message := <-c.Input:
			c.handleMessage(message)
		}
	}
}

func (c *NdtpMaster) sendFirstMessage() error {
	firstMessage, err := db.ReadConnDB(c.pool, c.terminalID, c.logger)
	if err != nil {
		return err
	}
	return c.send2Server(firstMessage)
}

func (c *NdtpMaster) handleMessage(message []byte) {
	data := util.Deserialize(message)
	packet := data.Packet
	service, err := ndtp.Service(data.Packet)
	if err != nil {
		c.logger.Errorf("can't get service: %s", err)
		return
	}
	if service == ndtp.NphSrvNavdata {
		nphID, err := c.getNphID()
		if err != nil {
			c.logger.Errorf("can't get NPH ID: %v", err)
			return
		}
		changes := map[string]int{ndtp.NphReqID: int(nphID)}
		newPacket := ndtp.Change(packet, changes)
		err = db.WriteNDTPid(c.pool, c.id, c.terminalID, nphID, message[:util.PacketStart], c.logger)
		if err != nil {
			c.logger.Errorf("can't write NDTP id: %s", err)
			return
		}
		err = c.send2Server(newPacket)
		if err != nil {
			c.logger.Warningf("can't send to NDTP server: %s", err)
			c.connStatus()
		}
	} else {
		err := c.send2Server(packet)
		if err != nil {
			c.logger.Warningf("can't send to NDTP server: %s", err)
			c.connStatus()
		}
	}
}

func (c *NdtpMaster) replyHandler() {
	var buf []byte
	for {
		//check if server is closed
		if c.serverClosed() {
			return
		}
		if c.open {
			buf = c.waitServerMessage(buf)
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func (c *NdtpMaster) waitServerMessage(buf []byte) []byte {
	err := c.conn.SetReadDeadline(time.Now().Add(readTimeout))
	if err != nil {
		c.logger.Warningf("can't SetReadDeadLine: %s", err)
	}
	var b [defaultBufferSize]byte
	n, err := c.conn.Read(b[:])
	if err != nil {
		c.logger.Warningf("can't get data from server: %v", err)
		c.connStatus()
		time.Sleep(5 * time.Second)
		return nil
	}
	util.PrintPacket(c.logger, "received packet from server ", b[:n])
	buf = append(buf, b[:n]...)
	buf, err = c.processPacket(buf)
	if err != nil {
		c.logger.Warningf("can't process packet: %s", err)
		if len(buf) > 1024 {
			return []byte{}
		}
	}
	return buf
}

func (c *NdtpMaster) processPacket(buf []byte) ([]byte, error) {
	//c.logger.Tracef("start process packet: %d, %d", len(buf), len(rest))
	for len(buf) > 0 {
		//c.logger.Tracef("process packet: %v", buf)
		var service, packetType uint16
		var err error
		var packet []byte
		packet, buf, service, packetType, _, err = ndtp.SimpleParse(buf)
		c.logger.Tracef("packet: %d buf: %d service: %d packetType: %d", len(packet), len(buf), service, packetType)
		if err != nil {
			return buf, err
		}
		if service == 1 && packetType == 0 {
			err = c.handleResult(packet)
			if err != nil {
				return []byte{}, err
			}
		} else if service == 0 && packetType == 0 {
			if c.auth {
				c.send2Channel(c.Output, packet)
			} else {
				c.logger.Tracef("received auth reply")
				c.auth = true
			}
			continue
		} else {
			c.send2Channel(c.Output, packet)
		}
	}
	return buf, nil
}

func (c *NdtpMaster) handleResult(packet []byte) (err error) {
	packetData := new(ndtp.Packet)
	_, err = packetData.Parse(packet)
	if err != nil {
		return
	}
	res := packetData.Nph.Data.(uint32)
	if res == ndtp.NphResultOk {
		err = db.ConfirmNdtp(c.pool, c.terminalID, packetData.Nph.ReqID, c.id, c.logger)
	} else {
		c.logger.Warningf("got nph result error: %d", res)
	}
	return
}

func (c *NdtpMaster) old() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.exitChan:
			return
		case <-ticker.C:
			c.checkOld()
		}
	}
}

func (c *NdtpMaster) checkOld() {
	res, err := db.OldPacketsNdtp(c.pool, c.id, c.terminalID, c.logger)
	if err != nil {
		c.logger.Warningf("can't get old NDTP packets: %s", err)
	} else {
		c.resend(res)
	}
	return
}

func (c *NdtpMaster) resend(messages [][]byte) {
	messages = reverceSlice(messages)
	for _, mes := range messages {
		data := util.Deserialize(mes)
		packet := data.Packet
		nphID, err := c.getNphID()
		if err != nil {
			c.logger.Errorf("can't get NPH ID: %v", err)
		}
		changes := map[string]int{ndtp.NphReqID: int(nphID), ndtp.PacketType: 100}
		newPacket := ndtp.Change(packet, changes)
		util.PrintPacket(c.logger, "packet after changing: ", newPacket)
		err = db.WriteNDTPid(c.pool, c.id, c.terminalID, nphID, mes[:util.PacketStart], c.logger)
		if err != nil {
			c.logger.Errorf("can't write NDTP id: %s", err)
			return
		}
		util.PrintPacket(c.logger, "send packet to server: ", newPacket)
		err = c.send2Server(newPacket)
		if err != nil {
			c.logger.Warningf("can't send to NDTP server: %s", err)
			c.connStatus()
		}
	}
}

func (c *NdtpMaster) send2Server(packet []byte) error {
	util.PrintPacket(c.logger, "send message to server: ", packet)
	if c.open {
		return send(c.conn, packet)
	}
	c.connStatus()
	return errors.New("connection to server is closed")
}

func (c *NdtpMaster) getNphID() (uint32, error) {
	c.mu.Lock()
	nphID := c.nphID
	c.nphID++
	err := db.SetNph(c.pool, c.id, c.terminalID, c.nphID, c.logger)
	c.mu.Unlock()
	return nphID, err
}
func (c *NdtpMaster) connStatus() {
	c.muRecon.Lock()
	defer c.muRecon.Unlock()
	//if s.servConn.closed || s.servConn.recon {
	if !c.open || c.reconnecting {
		return
	}
	c.reconnecting = true
	if err := c.conn.Close(); err != nil {
		c.logger.Debugf("can't close servConn: %s", err)
	}
	c.open = false
	go c.reconnect()
}

func (c *NdtpMaster) reconnect() {
	c.logger.Printf("start reconnecting NDTP")
	for {
		for i := 0; i < 3; i++ {
			if c.serverClosed() {
				c.logger.Println("close because server is closed")
				return
			}
			conn, err := net.Dial("tcp", c.address)
			if err != nil {
				c.logger.Warningf("can't reconnect: %s", err)
			} else {
				c.logger.Printf("start sending first message again")
				firstMessage, err := db.ReadConnDB(c.pool, c.terminalID, c.logger)
				if err != nil {
					c.logger.Errorf("can't readConnDB: %s", err)
					return
				}
				util.PrintPacket(c.logger, "send first message again: ", firstMessage)
				err = send(conn, firstMessage)
				if err == nil {
					c.logger.Printf("reconnected")
					c.conn = conn
					c.open = true
					time.Sleep(1 * time.Minute)
					c.reconnecting = false
					return
				}
				c.logger.Warningf("failed sending first message again to NDTP server: %s", err)
			}
		}
		time.Sleep(1 * time.Minute)
	}
}

func (c *NdtpMaster) serverClosed() bool {
	select {
	case <-c.exitChan:
		return true
	default:
		return false
	}
}

func (c *NdtpMaster) send2Channel(channel chan []byte, data []byte) {
	select {
	case channel <- data:
		return
	default:
		c.logger.Warningln("channel is full")
	}
}

func (c *NdtpMaster) setNph() error {
	nph, err := db.GetNph(c.pool, c.id, c.terminalID, c.logger)
	if err == nil {
		c.nphID = nph
	}
	return err
}
