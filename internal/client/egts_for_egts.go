package client

import (
	"encoding/binary"
	"time"

	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/monitoring"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/egorban/navprot/pkg/egts"
)

func (c *Egts) clientLoop4Egts() {
	dbConn := db.Connect(c.DB)
	defer c.closeDBConn(dbConn)
	err := c.getID(dbConn)
	if err != nil {
		c.logger.Errorf("can't getID: %v", err)
	}
	var buf []byte
	var records [][]byte
	var countRec int
	var countPack int
	sendTicker := time.NewTicker(100 * time.Millisecond)
	defer sendTicker.Stop()
	for {
		if c.open {
			select {
			case message := <-c.Input:
				monitoring.SendMetric(c.Options, c.name, monitoring.QueuedRecs, len(c.Input))
				if db.CheckOldData(dbConn, message[:util.PacketStartEgts], c.logger) {
					continue
				}
				record := c.processMessage4Egts(dbConn, message)
				if record != nil {
					records = append(records, record)
					countRec++
				}
				if countRec == recsAtPacketEgts {
					buf, err = c.formPacketEgts(records, buf)
					if err == nil {
						countPack++
					}
					records = [][]byte(nil)
					countRec = 0
				}
				if countPack == numPcktsToSend {
					err := c.send(buf)
					if err == nil {
						monitoring.SendMetric(c.Options, c.name, monitoring.SentPkts, countPack)
						monitoring.SendMetric(c.Options, c.name, monitoring.SentRecs, countPack*recsAtPacketEgts)
					}
					buf = []byte(nil)
					countPack = 0
				}
			case <-sendTicker.C:
				if (countRec > 0) && (countRec < recsAtPacketEgts) {
					buf, err = c.formPacketEgts(records, buf)
					if err == nil {
						countPack++
					}
					records = [][]byte(nil)
					countRec = 0
				}
				if (countPack > 0) && (countPack <= numPcktsToSend) {
					err := c.send(buf)
					if err == nil {
						monitoring.SendMetric(c.Options, c.name, monitoring.SentPkts, countPack)
						monitoring.SendMetric(c.Options, c.name, monitoring.SentRecs, countPack*recsAtPacketEgts)
					}
					buf = []byte(nil)
					countPack = 0
				}
			}
		} else {
			time.Sleep(time.Duration(TimeoutClose) * time.Second)
			buf = []byte(nil)
			records = [][]byte(nil)
			countRec = 0
			countPack = 0
		}
	}
}

func (c *Egts) processMessage4Egts(dbConn db.Conn, message []byte) []byte {
	util.PrintPacket(c.logger, "serialized data: ", message)
	data := util.Deserialize4Egts(message)
	c.logger.Tracef("data: %+v", data)
	recID, err := c.idRec(dbConn)
	if err != nil {
		c.logger.Errorf("can't get ids: %s", err)
		return []byte(nil)
	}
	record := changeRecNum(recID, data.Record)
	err = db.WriteEgtsID(dbConn, c.id, recID, data.ID)
	if err != nil {
		c.logger.Errorf("error WriteEgtsID: %s", err)
	}
	return record
}

func changeRecNum(recID uint16, record []byte) []byte {
	changedRec := make([]byte, len(record))
	copy(changedRec, record)
	binary.LittleEndian.PutUint16(changedRec[2:4], recID)
	return changedRec
}

func (c *Egts) idRec(conn db.Conn) (uint16, error) {
	c.mu.Lock()
	egtsRecID := c.egtsRecID
	c.egtsRecID++
	err := db.SetEgtsID(conn, c.id, c.egtsRecID)
	c.mu.Unlock()
	return egtsRecID, err
}

func (c *Egts) idPack() uint16 {
	c.mu.Lock()
	egtsMessageID := c.egtsMessageID
	c.egtsMessageID++
	c.mu.Unlock()
	return egtsMessageID
}

func (c *Egts) formPacketEgts(recordsBin [][]byte, buf []byte) ([]byte, error) {
	id := c.idPack()
	var records []*egts.Record
	for _, recBin := range recordsBin {
		record := &egts.Record{
			RecBin: recBin,
		}
		records = append(records, record)
	}
	packetData := egts.Packet{
		Type:    egts.EgtsPtAppdata,
		ID:      id,
		Records: records,
		Data:    nil,
	}
	pack, err := packetData.Form()
	if err != nil {
		return buf, err
	}
	buf = append(buf, pack...)
	return buf, nil
}

func (c *Egts) old4Egts() {
	dbConn := db.Connect(c.DB)
	ticker := time.NewTicker(time.Duration(PeriodCheckOld) * time.Second)
	defer ticker.Stop()
OLDLOOP:
	for {
		if c.open {
			<-ticker.C
			c.logger.Debugf("start checking old data")
			messages, err := db.OldPacketsEGTS(dbConn, c.id, util.PacketStartEgts)
			if err != nil {
				c.logger.Warningf("can't get old packets: %s", err)
				continue
			}
			c.logger.Debugf("get %d old packets", len(messages))
			var buf []byte
			var records [][]byte
			var countRec int
			var countPack int
			for _, msg := range messages {
				record := c.processMessage4Egts(dbConn, msg)
				if record != nil {
					records = append(records, record)
					countRec++
				}
				if countRec == recsAtPacketEgts {
					buf, err = c.formPacketEgts(records, buf)
					if err == nil {
						countPack++
					}
					records = [][]byte(nil)
					countRec = 0
				}
				if countPack > numPcktsToSend-1 {
					c.logger.Debugf("send old EGTS packets to EGTS server: %v", buf)
					if err = c.send(buf); err != nil {
						c.logger.Infof("can't send packet to EGTS server: %v; %v", err, buf)
						continue OLDLOOP
					}
					monitoring.SendMetric(c.Options, c.name, monitoring.SentPkts, countPack)
					monitoring.SendMetric(c.Options, c.name, monitoring.SentRecs, countPack*recsAtPacketEgts)
					countPack = 0
					buf = []byte(nil)
				}
			}
			if countRec > 0 {
				buf, err = c.formPacketEgts(records, buf)
				if err == nil {
					countPack++
				}
				records = [][]byte(nil)
				countRec = 0
			}
			if len(buf) > 0 {
				c.logger.Debugf("oldEGTS: send rest packets to EGTS server: %v", buf)
				err := c.send(buf)
				if err == nil {
					monitoring.SendMetric(c.Options, c.name, monitoring.SentPkts, countPack)
					monitoring.SendMetric(c.Options, c.name, monitoring.SentRecs, countPack*recsAtPacketEgts)
				}
			}
		} else {
			time.Sleep(time.Duration(TimeoutClose) * time.Second)
		}
	}
}
