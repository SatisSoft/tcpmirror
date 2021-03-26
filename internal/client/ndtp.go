package client

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/monitoring"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/egorban/navprot/pkg/ndtp"
	"github.com/sirupsen/logrus"
)

// NdtpChanSize defines size of Ndtp client input chanel buffer
const NdtpChanSize = 10
const NdtpOldChanSize = 10000

type ndtpSession struct {
	nplID uint16
	nphID uint32
	mu    sync.Mutex
}

// Ndtp describes Ndtp client
type Ndtp struct {
	Input      chan []byte
	exitChan   chan struct{}
	pool       *db.Pool
	terminalID int
	auth       bool
	*info
	*ndtpSession
	*connection
	confChan  chan *db.ConfMsg
	OldInput  chan []byte
	finishOld chan int
}

// NewNdtp creates new Ndtp client
func NewNdtp(sys util.System, options *util.Options, pool *db.Pool, exitChan chan struct{},
	confChan chan *db.ConfMsg) *Ndtp {
	c := new(Ndtp)
	c.info = new(info)
	c.ndtpSession = new(ndtpSession)
	c.connection = new(connection)
	c.id = sys.ID
	c.name = sys.Name
	c.address = sys.Address
	c.logger = logrus.WithFields(logrus.Fields{"type": "ndtp_client", "vis": sys.ID})
	c.Options = options
	c.Input = make(chan []byte, NdtpChanSize)
	c.exitChan = exitChan
	c.pool = pool
	c.confChan = confChan
	c.OldInput = make(chan []byte, NdtpOldChanSize)
	c.finishOld = make(chan int, 1)
	return c
}

func (c *Ndtp) start() {
	c.logger = c.logger.WithFields(logrus.Fields{"terminalID": c.terminalID})
	err := c.setNph()
	if err != nil {
		c.logger.Errorf("can't setNph: %v", err)
	}
	c.logger.Traceln("start")
	conn, err := net.Dial("tcp", c.address)
	if err != nil {
		c.logger.Errorf("error while connecting to NDTP server %d: %s", c.id, err)
		c.reconnect()
	} else {
		c.logger = c.logger.WithFields(logrus.Fields{"dstAddr": conn.RemoteAddr().String()})
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
func (c *Ndtp) InputChannel() chan []byte {
	return c.Input
}

// OutputChannel implements method of Client interface
func (c *Ndtp) OutputChannel() chan []byte {
	return nil
}

// SetID sets terminalID
func (c *Ndtp) SetID(terminalID int) {
	c.terminalID = terminalID
}

func (c *Ndtp) authorization() error {
	time.Sleep(100 * time.Millisecond)
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

func (c *Ndtp) clientLoop() {
	ticker := time.NewTicker(time.Duration(PeriodSendOnlyOldNdtpMs) * time.Millisecond)
	for {
		if c.open {
			select {
			case <-c.exitChan:
				c.closeConn()
				return
			case message := <-c.Input:
				ticker.Stop()
				monitoring.SendMetric(c.Options, c.name, monitoring.QueuedPkts, len(c.Input))
				c.handleMessage(message)
				ticker = time.NewTicker(time.Duration(PeriodSendOnlyOldNdtpMs) * time.Millisecond)
			case <-ticker.C:
				ticker.Stop()
				c.sendOldPackets()
				ticker = time.NewTicker(time.Duration(PeriodSendOnlyOldNdtpMs) * time.Millisecond)
			}
		} else {
			time.Sleep(time.Duration(TimeoutCloseSec) * time.Second)
		}
	}
}

func (c *Ndtp) sendFirstMessage() error {
	firstMessage, err := db.ReadConnDB(c.pool, c.terminalID, c.logger)
	if err != nil {
		return err
	}
	return c.send2Server(firstMessage)
}

func (c *Ndtp) handleMessage(message []byte) {
	if db.IsOldData(c.pool, message[:util.PacketStart], c.logger) {
		return
	}
	data := util.Deserialize(message)
	packet := data.Packet
	nphID, err := c.getNphID()
	c.logger.Tracef("set nphID %d to packet %v", nphID, packet)
	if err != nil {
		c.logger.Errorf("can't get NPH ID: %v", err)
		return
	}
	changes := map[string]int{ndtp.NphReqID: int(nphID)}
	newPacket := ndtp.Change(packet, changes)
	err = db.WriteNDTPid(c.pool, c.id, c.terminalID, nphID, message[:util.PacketStart], c.logger)
	if err != nil {
		c.logger.Errorf("can't write NDTP id: %v", err)
		return
	}
	err = c.send2Server(newPacket)
	if err != nil {
		c.logger.Warningf("can't send to NDTP server: %v", err)
		c.connStatus()
	}
	c.sendOldPackets()
}

func (c *Ndtp) sendOldPackets() {
	c.logger.Infof("sendOldPackets %v", len(c.OldInput))
	num := 0
	for len(c.OldInput) > 0 && num < 10 {
		oldPacket := <-c.OldInput
		data := util.Deserialize(oldPacket)
		packet := data.Packet
		nphID, err := c.getNphID()
		if err != nil {
			c.logger.Errorf("can't get NPH ID: %v", err)
		}
		changes := map[string]int{ndtp.NphReqID: int(nphID), ndtp.PacketType: 100}
		newPacket := ndtp.Change(packet, changes)
		util.PrintPacket(c.logger, "old packet after changing: ", newPacket)
		err = db.WriteNDTPid(c.pool, c.id, c.terminalID, nphID, oldPacket[:util.PacketStart], c.logger)
		if err != nil {
			c.logger.Errorf("can't write NDTP id: %s", err)
			return
		}
		c.logger.Infoln("send old packet to server 1 num:", num)
		util.PrintPacket(c.logger, "send old packet to server: ", newPacket)
		err = c.send2Server(newPacket)
		if err != nil {
			c.logger.Warningf("can't send old to NDTP server: %s", err)
			c.connStatus()
		}
		num++
	}

	if len(c.OldInput) == 0 && num > 0 {
		c.logger.Infof("finish send old")
		c.finishOld <- WaitConfNdtpMs
	}
}

func (c *Ndtp) replyHandler() {
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

func (c *Ndtp) waitServerMessage(buf []byte) []byte {
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
		if len(buf) > defaultBufferSize {
			return []byte{}
		}
	}
	return buf
}

func (c *Ndtp) processPacket(buf []byte) ([]byte, error) {
	var err error
	for len(buf) > 0 {
		c.logger.Tracef("process buff: %v", buf)
		packetData := new(ndtp.Packet)
		buf, err = packetData.Parse(buf)
		if err != nil {
			return buf, err
		}
		c.logger.Tracef("packet: %d buf: %d service: %d packetType: %s", len(packetData.Packet), len(buf), packetData.Service(), packetData.PacketType())
		if packetData.IsResult() && packetData.Service() == ndtp.NphSrvNavdata {
			err = c.handleResult(packetData)
			if err != nil {
				c.logger.Warningf("can't handle result: %v; %v", err, packetData)
			}
		} else {
			if packetData.IsResult() && packetData.Service() == ndtp.NphSrvGenericControls {
				if c.auth {
					c.logger.Warningf("expected result navdata, but received %v", packetData)
				} else {
					c.logger.Tracef("received auth reply")
					monitoring.SendMetric(c.Options, c.name, monitoring.RcvdPkts, 1)
					c.auth = true
				}
			} else {
				c.logger.Warningf("expected result navdata, but received %v", packetData)
			}
			continue
		}
	}
	return buf, nil
}

func (c *Ndtp) handleResult(packetData *ndtp.Packet) (err error) {
	res := packetData.Nph.Data.(uint32)
	if res == ndtp.NphResultOk {
		err = db.ConfirmNdtp(c.pool, c.terminalID, packetData.Nph.ReqID, c.id, c.logger, c.confChan)
	} else {
		c.logger.Warningf("got nph result error: %d", res)
	}
	return
}

func (c *Ndtp) old() {
	c.checkOld()
	for {
		if c.open {
			select {
			case <-c.exitChan:
				c.closeConn()
				return
			case sleepTimeMs := <-c.finishOld:
				time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
				c.checkOld()
			}
		} else {
			time.Sleep(time.Duration(TimeoutCloseSec) * time.Second)
		}
	}
}

func (c *Ndtp) checkOld() {
	limit := 1200
	maxToSend := 1200
	c.logger.Infof("start checking old: maxToSend = %v; limit = %v", maxToSend, limit)
	messages, err := db.OldPacketsNdtp(c.pool, c.id, c.terminalID, limit, maxToSend, c.logger)
	if err != nil {
		c.logger.Warningf("can't get old NDTP packets: %s", err)
		c.finishOld <- PeriodCheckOldNdtpMs
	} else {
		lenMessages := len(messages)
		c.logger.Infof("receive old: %v, %v", err, lenMessages)
		if lenMessages > 0 {
			messages = reverseSlice(messages)
			for _, mes := range messages {
				c.OldInput <- mes
			}
		} else {
			c.logger.Infof("finish check old")
			c.finishOld <- PeriodCheckOldNdtpMs
		}
	}
}

func reverseSlice(res [][]byte) [][]byte {
	for i := len(res)/2 - 1; i >= 0; i-- {
		opp := len(res) - 1 - i
		res[i], res[opp] = res[opp], res[i]
	}
	return res
}

func (c *Ndtp) send2Server(packet []byte) error {
	util.PrintPacket(c.logger, "send message to server: ", packet)
	if c.open {
		return c.send(packet)
	}
	c.connStatus()
	return errors.New("connection to server is closed")
}

func (c *Ndtp) send(packet []byte) error {
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

func (c *Ndtp) getNphID() (uint32, error) {
	c.logger.Tracef("getNphID")
	c.mu.Lock()
	nphID := c.nphID
	c.nphID++
	c.logger.Tracef("getNphID: %v", c.nphID)
	err := db.SetNph(c.pool, c.id, c.terminalID, c.nphID, c.logger)
	c.mu.Unlock()
	return nphID, err
}

func (c *Ndtp) connStatus() {
	c.muRecon.Lock()
	defer c.muRecon.Unlock()
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

func (c *Ndtp) reconnect() {
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
				c.logger = c.logger.WithFields(logrus.Fields{"dstAddr": conn.RemoteAddr().String()})
				c.conn = conn
				c.open = true
				err = c.authorization()
				if err == nil {
					c.logger.Printf("reconnected")
					go c.chanReconStatus()
					return
				}
				c.logger.Warningf("failed sending first message again to NDTP server: %s", err)
				if err := c.conn.Close(); err != nil {
					c.logger.Debugf("can't close servConn: %s", err)
				} else {
					c.logger.Printf("close servConn 4")
				}
			}
		}
		time.Sleep(1 * time.Minute)
	}
}

func (c *Ndtp) serverClosed() bool {
	select {
	case <-c.exitChan:
		c.logger.Println("close because server is closed 2")
		if err := c.conn.Close(); err != nil {
			c.logger.Debugf("can't close servConn: %s", err)
		} else {
			c.logger.Printf("close servConn 2")
		}
		return true
	default:
		return false
	}
}

func (c *Ndtp) setNph() error {
	nph, err := db.GetNph(c.pool, c.id, c.terminalID, c.logger)
	if err == nil {
		c.nphID = nph
	}
	return err
}

func (c *Ndtp) chanReconStatus() {
	time.Sleep(1 * time.Minute)
	c.reconnecting = false
}

func (c *Ndtp) closeConn() {
	if c.conn != nil {
		c.logger.Println("close because server is closed")
		if err := c.conn.Close(); err != nil {
			c.logger.Debugf("can't close servConn: %s", err)
		} else {
			c.logger.Printf("close servConn")
		}
	}
}
