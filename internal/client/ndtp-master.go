package client

import (
	"errors"
	"net"
	"time"

	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/monitoring"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/egorban/navprot/pkg/ndtp"
	"github.com/sirupsen/logrus"
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
	confChan chan *db.ConfMsg
}

// NewNdtpMaster creates new NdtpMaster client
func NewNdtpMaster(sys util.System, options *util.Options, pool *db.Pool, exitChan chan struct{},
	confChan chan *db.ConfMsg) *NdtpMaster {
	c := new(NdtpMaster)
	c.info = new(info)
	c.ndtpSession = new(ndtpSession)
	c.connection = new(connection)
	c.id = sys.ID
	c.name = sys.Name
	c.address = sys.Address
	c.logger = logrus.WithFields(logrus.Fields{"type": "ndtp_master_client", "vis": sys.ID})
	c.Options = options
	c.Input = make(chan []byte, NdtpMasterChanSize)
	c.Output = make(chan []byte, NdtpMasterChanSize)
	c.exitChan = exitChan
	c.pool = pool
	c.confChan = confChan
	return c
}

func (c *NdtpMaster) start() {
	c.logger = c.logger.WithFields(logrus.Fields{"terminalID": c.terminalID})
	err := c.setNph()
	if err != nil {
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
		if err = c.authorization(); err != nil {
			c.logger.Errorf("error authorization: %s", err)
		}
	}
	if c.serverClosed() {
		return
	}
	go c.old()
	go c.replyHandler()
	c.clientLoop()
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

func (c *NdtpMaster) authorization() error {
	c.logger.Traceln("start authorization")
	err := c.sendFirstMessage()
	if err != nil {
		return err
	}
	var b [defaultBufferSize]byte
	n, err := c.conn.Read(b[:])
	c.logger.Tracef("received auth reply from server: %v; %v", err, b[:n])
	if err != nil {
		return err
	}
	monitoring.SendMetric(c.Options, c.name, monitoring.RcvdBytes, n)
	_, err = c.processPacket(b[:n])
	if err != nil {
		return err
	}
	if !c.auth {
		return errors.New("didn't receive auth packet during authorization")
	}
	c.logger.Traceln("authorization succeeded")
	return nil
}

func (c *NdtpMaster) clientLoop() {
	for {
		if c.open {
			select {
			case <-c.exitChan:
				return
			case message := <-c.Input:
				monitoring.SendMetric(c.Options, c.name, monitoring.QueuedPkts, len(c.Input))
				c.handleMessage(message)
			}
		} else {
			time.Sleep(time.Duration(TimeoutClose) * time.Second)
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
		if db.IsOldData(c.pool, message[:util.PacketStart], c.logger) {
			return
		}
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
		c.logger.Tracef("send control packet to server: %v", packet)
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
	monitoring.SendMetric(c.Options, c.name, monitoring.RcvdBytes, n)
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
		c.logger.Tracef("process buff: %v", buf)
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
				c.logger.Warningf("can't handle result: %v; %v", err, packet)
			}
		} else if service == 0 && packetType == 0 {
			if c.auth {
				monitoring.SendMetric(c.Options, c.name, monitoring.RcvdPkts, 1)
				c.send2Channel(c.Output, packet)
			} else {
				c.logger.Tracef("received auth reply")
				monitoring.SendMetric(c.Options, c.name, monitoring.RcvdPkts, 1)
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
		err = db.ConfirmNdtp(c.pool, c.terminalID, packetData.Nph.ReqID, c.id, c.logger, c.confChan)
	} else {
		c.logger.Warningf("got nph result error: %d", res)
	}
	return
}

func (c *NdtpMaster) old() {
	ticker := time.NewTicker(time.Duration(PeriodCheckOld) * time.Second)
	c.logger.Traceln("start old ticker")
	c.checkOld()
	defer ticker.Stop()
	for {
		if c.open {
			select {
			case <-c.exitChan:
				return
			case <-ticker.C:
				c.checkOld()
			}
		} else {
			time.Sleep(time.Duration(TimeoutClose) * time.Second)
		}
	}
}

func (c *NdtpMaster) checkOld() {
	c.logger.Traceln("start checking old")
	res, err := db.OldPacketsNdtp(c.pool, c.id, c.terminalID, c.logger)
	c.logger.Tracef("receive old: %v, %v ", err, res)
	if err != nil {
		c.logger.Warningf("can't get old NDTP packets: %s", err)
	} else {
		c.resend(res)
	}
	return
}

func (c *NdtpMaster) resend(messages [][]byte) {
	messages = reverseSlice(messages)
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
			return
		}
	}
}

func (c *NdtpMaster) send2Server(packet []byte) error {
	util.PrintPacket(c.logger, "send message to server: ", packet)
	if c.open {
		return c.send(packet)
	}
	c.connStatus()
	return errors.New("connection to server is closed")
}

func (c *NdtpMaster) send(packet []byte) error {
	err := c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err != nil {
		return err
	}
	n, err := c.conn.Write(packet)
	if err == nil {
		monitoring.SendMetric(c.Options, c.name, monitoring.SentBytes, n)
		monitoring.SendMetric(c.Options, c.name, monitoring.SentPkts, 1)
	}
	return err
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
	c.auth = false
	c.reconnect()
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
				c.logger.Printf("start authorization")
				c.conn = conn
				c.open = true
				err = c.authorization()
				if err == nil {
					c.logger.Printf("reconnected")
					go c.chanReconStatus()
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

func (c *NdtpMaster) chanReconStatus() {
	time.Sleep(1 * time.Minute)
	c.reconnecting = false
}
