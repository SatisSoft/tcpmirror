package main

import (
	"encoding/binary"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"net"
	"strconv"
	"sync"
	"time"
)

func checkOldDataClient(cR redis.Conn, s *session, ndtpConn *connection, mu *sync.Mutex, id int, ErrNDTPCh chan error) {
	logger := s.logger
	res, err := getOldNDTP(cR, id)
	if err != nil {
		logger.Warningf("getOldNDTP %d : %v", id, err)
	} else {
		resendNav(cR, s, ndtpConn, mu, id, ErrNDTPCh, res)
	}
	res, err = getOldNDTPExt(cR, id)
	if err != nil {
		logger.Warningf("can't getOldNDTPExt: %v", err)
	} else {
		resendExt(cR, s, ndtpConn, mu, id, ErrNDTPCh, res)
	}
}

func resendNav(cR redis.Conn, s *session, ndtpConn *connection, mu *sync.Mutex, id int, ErrNDTPCh chan error, res [][]byte) {
	logger := s.logger
	for _, mes := range res {
		data, _, _, _ := parseNDTP(mes)
		if ndtpConn.closed != true {
			mill, err := getScore(cR, id, mes)
			if err != nil {
				logger.Warningf("can't get score for %v : %v", mes, err)
				continue
			}
			NPHReqID, message := changePacketHistory(mes, data, s)
			err = writeNDTPid(cR, s.id, NPHReqID, mill)
			if err != nil {
				logger.Errorf("error writeNDTPid: %v", err)
			} else {
				ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				printPacket(logger,"send message: ", message)
				_, err = ndtpConn.conn.Write(message)
				if err != nil {
					logger.Warningf("can't send to NDTP server: %s", err)
					ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
				} else if enableMetrics {
					countToServerNDTP.Inc(1)
				}
			}
		} else {
			logger.Debugf("connection to server closed")
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func resendExt(cR redis.Conn, s *session, ndtpConn *connection, mu *sync.Mutex, id int, ErrNDTPCh chan error, res [][]byte) {
	logger := s.logger
	for _, mes := range res {
		var data ndtpData
		data, _, _, _ = parseNDTP(mes)
		if ndtpConn.closed != true {
			mill, err := getScoreExt(cR, id, mes)
			if err != nil {
				logger.Warningf("can't get score for ext %v : %v", mes, err)
				continue
			}
			if data.NPH.NPHType == NPH_SED_DEVICE_RESULT {
				_, message := changePacket(mes, s)
				printPacket(logger,"resendExt: send message: ", message)
				ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				_, err = ndtpConn.conn.Write(message)
				if err != nil {
					logger.Warningf("can't send to NDTP server: %s", err)
					ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
				} else {
					logger.Debugln("remove old data")
					removeOldExt(cR, id, mill)
					if enableMetrics {
						countToServerNDTP.Inc(1)
					}
				}
			} else if data.NPH.NPHType == NPH_SED_DEVICE_TITLE_DATA {
				_, message := changePacket(mes, s)
				printPacket(logger,"resendExt: packet after changing ext device message: ", message)
				err = writeNDTPIdExt(cR, s.id, data.ext.mesID, mill)
				if err != nil {
					logger.Warningf("can't writeNDTPIdExt: %v", err)
				} else {
					printPacket(logger,"send ext device message to server: ", message)
					ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
					_, err = ndtpConn.conn.Write(message)
					if err != nil {
						logger.Warningf("can't send ext device message to NDTP server: %s", err)
						ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
					} else if enableMetrics {
						countToServerNDTP.Inc(1)
					}
				}
			}
		} else {
			logger.Debugf("connection to server closed")
			return
		}
	}
}

func checkOldDataServ(cR redis.Conn, s *session, client net.Conn, id int) {
	logger := s.logger
	res, mill, flag, _, err := getServExt(cR, id)
	if err != nil {
		logger.Warningf("can't getServExt: %v", err)
		return
	}
	logger.Debugf("mill: %d; flag: %s; res: %v", mill, flag, res)
	now := getMill()
	if now-mill > 60000 && flag == "0" {
		err = removeServerExt(cR, s.id)
		if err != nil {
			logger.Warningf("can't remove old ext data: %v", err)
		}
		return
	}
	err = setFlagServerExt(cR, s.id, "0")
	if err != nil {
		logger.Warningf("can't setNDTPExtFlag: %s", err)
	}
	_, message := changePacketFromServ(res, s)
	printPacket(logger,"send ext device message to client: ", message)
	client.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = client.Write(message)
	if err != nil {
		logger.Warningf("can't send ext device message to NDTP client: %s", err)
	}
}

func ndtpRemoveExpired(s * session, ErrNDTPCh chan error) {
	logger := s.logger
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			logger.Errorf("can't connect to redis: %s", err)
		} else {
			break
		}
	}
	defer cR.Close()
	for {
		select {
		case <-ErrNDTPCh:
			return
		default:
			time.Sleep(1 * time.Hour)
		}
		err := removeExpiredDataNDTP(cR, s.id)
		if err != nil {
			logger.Warning("can't remove expired data ndtp: %s", err)
			for {
				cR, err = redis.Dial("tcp", ":6379")
				if err != nil {
					logger.Errorf("can't reconnect to redis: %s", err)
				} else {
					break
				}
				time.Sleep(1 * time.Minute)
			}
		}
	}
}

func egtsRemoveExpired() {
	logger := logrus.WithField("egts", "expired")
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			logger.Warningf("error connecting to redis in egtsRemoveExpired: %s", err)
		} else {
			break
		}
	}
	defer cR.Close()
	for {
		err := removeExpiredDataEGTS(cR)
		if err != nil {
			logger.Warningf("can't remove expired EGTS data: %s", err)
			for {
				cR, err = redis.Dial("tcp", ":6379")
				if err != nil {
					logger.Warningf("can't reconnect to redis: %s", err)
				} else {
					break
				}
				time.Sleep(1 * time.Minute)
			}
		}
		time.Sleep(1 * time.Hour)
	}
}

func checkOldDataEGTS(cR redis.Conn, egtsMessageID, egtsReqID *uint16) {
	logger := logrus.WithField("egts", "old")
	logger.Debugf("start checking old data")
	messages, err := getOldEGTS(cR)
	if err != nil {
		logger.Warningf("can't get old EGTS %s", err)
		return
	}
	var bufOld []byte
	var i int
	for _, msg := range messages {
		if i < 10 {
			logger.Debugf("try to send message %v", msg)
			var dataNDTP ndtpData
			id := binary.LittleEndian.Uint32(msg)
			msgCopy := copyPack(msg)
			dataNDTP, _, _, err = parseNDTP(msgCopy)
			if err == nil {
				dataNDTP.ToRnis.id = id
				mill, err := getEGTSScore(cR, msg)
				dataNDTP.ToRnis.messageID = strconv.Itoa(int(id)) + ":" + strconv.FormatInt(mill, 10)
				packet := formEGTS(dataNDTP.ToRnis, *egtsMessageID, *egtsReqID)
				bufOld = append(bufOld, packet...)
				logger.Debugf("writeEGTSid %d : %s", *egtsMessageID, dataNDTP.ToRnis.messageID)
				err = writeEGTSid(cR, *egtsMessageID, dataNDTP.ToRnis.messageID)
				if err != nil {
					logger.Warningf("can't writeEGTSid: %v", err)
					continue
				}
				*egtsMessageID++
				*egtsReqID++
			} else {
				logger.Errorf("parse NDTP error: %v", err)
			}
			i++
		} else {
			logger.Debugf("send old EGTS packets to EGTS server: %v", bufOld)
			i = 0
			send2egts(bufOld, logger)
			bufOld = []byte(nil)
		}
	}
	if len(bufOld) > 0 {
		logger.Debugf("checkOldDataEGTS: send rest packets to EGTS server: %v", bufOld)
		send2egts(bufOld, logger)
	}
}
