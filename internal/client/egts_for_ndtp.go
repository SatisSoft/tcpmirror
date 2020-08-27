package client

import (
	"time"

	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/monitoring"
	"github.com/ashirko/tcpmirror/internal/util"
)

func (c *Egts) clientLoop4Ndtp() {
	dbConn := db.Connect(c.DB)
	defer c.closeDBConn(dbConn)
	err := c.getID(dbConn)
	if err != nil {
		c.logger.Errorf("can't getID: %v", err)
	}
	var buf []byte
	count := 0
	sendTicker := time.NewTicker(100 * time.Millisecond)
	defer sendTicker.Stop()
	for {
		if c.open {
			select {
			case message := <-c.Input:
				monitoring.SendMetric(c.Options, c.name, monitoring.QueuedPkts, len(c.Input))
				if db.CheckOldData(dbConn, message[:util.PacketStart], c.logger) {
					continue
				}
				buf = c.processMessage4Ndtp(dbConn, message, buf)
				count++
				if count == 10 {
					err := c.send(buf)
					if err == nil {
						monitoring.SendMetric(c.Options, c.name, monitoring.SentPkts, count)
					}
					buf = []byte(nil)
					count = 0
				}
			case <-sendTicker.C:
				if (count > 0) && (count < 10) {
					err := c.send(buf)
					if err == nil {
						monitoring.SendMetric(c.Options, c.name, monitoring.SentPkts, count)
					}
					buf = []byte(nil)
					count = 0
				}
			}
		} else {
			time.Sleep(time.Duration(TimeoutClose) * time.Second)
			buf = []byte(nil)
			count = 0
		}
	}
}

func (c *Egts) processMessage4Ndtp(dbConn db.Conn, message []byte, buf []byte) []byte {
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

func (c *Egts) old4Ndtp() {
	dbConn := db.Connect(c.DB)
	ticker := time.NewTicker(time.Duration(PeriodCheckOld) * time.Second)
	defer ticker.Stop()
OLDLOOP:
	for {
		if c.open {
			<-ticker.C
			c.logger.Debugf("start checking old data")
			messages, err := db.OldPacketsEGTS(dbConn, c.id, util.PacketStart)
			if err != nil {
				c.logger.Warningf("can't get old packets: %s", err)
				continue
			}
			c.logger.Debugf("get %d old packets", len(messages))
			var buf []byte
			var i int
			for _, msg := range messages {
				buf = c.processMessage4Ndtp(dbConn, msg, buf)
				i++
				if i > 9 {
					c.logger.Debugf("send old EGTS packets to EGTS server: %v", buf)
					if err = c.send(buf); err != nil {
						c.logger.Infof("can't send packet to EGTS server: %v; %v", err, buf)
						continue OLDLOOP
					}
					monitoring.SendMetric(c.Options, c.name, monitoring.SentPkts, i)
					i = 0
					buf = []byte(nil)
				}
			}
			if len(buf) > 0 {
				c.logger.Debugf("oldEGTS: send rest packets to EGTS server: %v", buf)
				err := c.send(buf)
				if err == nil {
					monitoring.SendMetric(c.Options, c.name, monitoring.SentPkts, i)
				}
			}
		} else {
			time.Sleep(time.Duration(TimeoutClose) * time.Second)
		}
	}
}
