package main

import (
	"encoding/binary"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"strconv"
	"time"
)

import nav "github.com/ashirko/navprot"

// resend old unconfirmed messages from client to server every 60 seconds
func oldFromClient(s *session) {
	oldTicker := time.NewTicker(60 * time.Second)
	defer oldTicker.Stop()
	for {
		select {
		case <-s.errClientCh:
			return
		case <-oldTicker.C:
			checkOldFromClient(s)
		}
	}
}

func checkOldFromClient(s *session) {
	conn := pool.Get()
	defer conn.Close()
	res, err := getOldNDTP(conn, s)
	if err != nil {
		s.logger.Warningf("getOldNDTP: %v", err)
	} else {
		resendNav(conn, s, res)
	}
	res, err = getOldNDTPExt(conn, s)
	if err != nil {
		s.logger.Warningf("can't getOldNDTPExt: %v", err)
	} else {
		resendExt(conn, s, res)
	}
	return
}

func resendNav(conn redis.Conn, s *session, res [][]byte) {
	res = reverceSlice(res)
	for _, mes := range res {
		ndtp := new(nav.NDTP)
		_, err := ndtp.Parse(mes)
		if err != nil {
			s.logger.Errorf("error parsing old ndtp: %s", err)
			continue
		}
		if s.servConn.closed != true {
			mill, err := getScore(conn, s, mes)
			if err != nil {
				s.logger.Warningf("can't get score for %v : %v", mes, err)
				continue
			}
			nphReqID := s.serverNPHReqID
			changes := map[string]int{nav.NplReqID: int(s.serverNplID()), nav.NphReqID: int(nphReqID)}
			if ndtp.Service() == nav.NphSrvNavdata && ndtp.PacketType() == nav.NphSndRealtime {
				s.logger.Infoln("change packet type to history")
				changes[nav.PacketType] = 100
			}
			ndtp.ChangePacket(changes)
			err = writeNDTPid(conn, s, nphReqID, mill)
			if err != nil {
				s.logger.Errorf("error writeNDTPid: %s", err)
			} else {
				printPacket(s.logger, "send message: ", ndtp.Packet)
				err = sendToServer(s, ndtp)
				if err != nil {
					s.logger.Warningf("can't send to NDTP server: %s", err)
					ndtpConStatus(s)
				} else if enableMetrics {
					countToServerNDTP.Inc(1)
				}
			}
		} else {
			s.logger.Debugf("connection to server closed")
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func resendExt(conn redis.Conn, s *session, res [][]byte) {
	res = reverceSlice(res)
	for _, mes := range res {
		ndtp := new(nav.NDTP)
		_, err := ndtp.Parse(mes)
		if err != nil {
			s.logger.Errorf("error parsing old ndtp: %s", err)
			continue
		}
		if s.servConn.closed != true {
			mill, err := getScoreExt(conn, s, mes)
			if err != nil {
				s.logger.Warningf("can't get score for ext %v : %v", mes, err)
				continue
			}
			pType := ndtp.PacketType()
			if pType == nav.NphSedDeviceResult {
				changes := map[string]int{nav.NplReqID: int(s.serverNplID()), nav.NphReqID: int(s.serverNPHReqID)}
				ndtp.ChangePacket(changes)
				printPacket(s.logger, "resendExt: send message: ", ndtp.Packet)
				err = sendToServer(s, ndtp)
				if err != nil {
					s.logger.Warningf("can't send to NDTP server: %s", err)
					ndtpConStatus(s)
				} else {
					s.logger.Debugln("remove old data")
					err = removeOldExt(conn, s, mill)
					if err != nil {
						s.logger.Errorf("can't remove old ext: %s", err)
					}
					if enableMetrics {
						countToServerNDTP.Inc(1)
					}
				}
			} else if pType == nav.NphSedDeviceTitleData {
				changes := map[string]int{nav.NplReqID: int(s.serverNplID()), nav.NphReqID: int(s.serverNPHReqID)}
				ndtp.ChangePacket(changes)
				printPacket(s.logger, "resendExt: packet after changing ext device message: ", ndtp.Packet)
				err = writeNDTPIdExt(conn, s, ndtp.Nph.Data.(*nav.ExtDevice).MesID, mill)
				if err != nil {
					s.logger.Warningf("can't writeNDTPIdExt: %v", err)
				} else {
					printPacket(s.logger, "send ext device message to server: ", ndtp.Packet)
					err = sendToServer(s, ndtp)
					if err != nil {
						s.logger.Warningf("can't send ext device message to NDTP server: %s", err)
						ndtpConStatus(s)
					} else if enableMetrics {
						countToServerNDTP.Inc(1)
					}
				}
			}
		} else {
			s.logger.Debugf("connection to server closed")
			return
		}
	}
}

func reverceSlice(res [][]byte) [][]byte {
	for i := len(res)/2 - 1; i >= 0; i-- {
		opp := len(res) - 1 - i
		res[i], res[opp] = res[opp], res[i]
	}
	return res
}

func oldFromServer(s *session) {
	checkTicker := time.NewTicker(60 * time.Second)
	defer checkTicker.Stop()
	for {
		select {
		case <-s.errClientCh:
			return
		case <-checkTicker.C:
			oldExtFromServer(s)
		}
	}
}

func oldExtFromServer(s *session) {
	conn := pool.Get()
	defer conn.Close()
	res, mill, flag, _, err := getServExt(conn, s)
	if err != nil {
		s.logger.Warningf("can't getServExt: %v", err)
		return
	}
	s.logger.Debugf("mill: %d; flag: %s; res: %v", mill, flag, res)
	now := getMill()
	if now-mill > 60000 && flag == "0" {
		err = removeServerExtOld(conn, s)
		if err != nil {
			s.logger.Warningf("can't remove old ext data: %v", err)
		}
		return
	}
	err = setFlagServerExt(conn, s, "0")
	if err != nil {
		s.logger.Warningf("can't setNDTPExtFlag: %s", err)
	}
	nphReqID := int(s.clientNphID())
	changes := map[string]int{nav.NplReqID: int(s.clientNplID()), nav.NphReqID: nphReqID}
	ndtp := new(nav.NDTP)
	ndtp.Packet = res
	ndtp.ChangePacket(changes)
	printPacket(s.logger, "send ext device message to client: ", ndtp.Packet)
	err = sendToClient(s, ndtp.Packet)
	if err != nil {
		s.logger.Warningf("can't send ext device message to NDTP client: %s", err)
	}
}

// delete very old unconfirmed messages from client to server every hour
func ndtpRemoveExpired(s *session) {
	expiredTicker := time.NewTicker(1 * time.Hour)
	defer expiredTicker.Stop()
	for {
		select {
		case <-s.errClientCh:
			return
		case <-expiredTicker.C:
			err := removeExpiredDataNDTP(s)
			if err != nil {
				s.logger.Errorf("can't remove expired data ndtp: %s", err)
			}
		}
	}
}

func egtsRemoveExpired() {
	logger := logrus.WithField("egts", "expired")
	expiredTicker := time.NewTicker(1 * time.Hour)
	defer expiredTicker.Stop()
	for {
		<-expiredTicker.C
		err := removeExpiredDataEGTS(logger)
		if err != nil {
			logger.Errorf("can't remove expired data egts: %s", err)
		}
	}
}

func oldEGTS(s *egtsSession) {
	cR := connRedis()
	defer cR.Close()
	checkTicker := time.NewTicker(60 * time.Second)
	defer checkTicker.Stop()
	logger := logrus.WithField("egts", "old")
	for {
		<-checkTicker.C
		logger.Debugf("start checking old data")
		messages, err := getOldEGTS(cR, logger)
		if err != nil {
			logger.Warningf("can't get old EGTS %s", err)
			return
		}
		var bufOld []byte
		var i int
		for _, msg := range messages {
			if i < 10 {
				bufOld = formEGTS(cR, s, bufOld, msg, logger)

				i++
			} else {
				logger.Debugf("send old EGTS packets to EGTS server: %v", bufOld)
				i = 0
				send2egts(bufOld, logger)
				bufOld = []byte(nil)
			}
		}
		if len(bufOld) > 0 {
			logger.Debugf("oldEGTS: send rest packets to EGTS server: %v", bufOld)
			send2egts(bufOld, logger)
		}

	}
}

func formEGTS(cR redis.Conn, s *egtsSession, bufOld []byte, msg []byte, logger *logrus.Entry) []byte {
	logger.Debugf("forming egts packet %v", msg)
	id := binary.LittleEndian.Uint32(msg)
	ndtp := new(nav.NDTP)
	msgCopy := append([]byte(nil), msg...)
	_, err := ndtp.Parse(msgCopy)
	if err != nil {
		logger.Errorf("error parsing old ndtp : %s", err)
		return bufOld
	}
	egts, err := nav.NDTPtoEGTS(*ndtp, uint32(id))
	if err == nil {
		mill, err := getEGTSScore(cR, msg, logger)
		if err != nil {
			logger.Errorf("error getEGTSScore: %s", err)
			return bufOld
		}
		messageID := strconv.Itoa(int(id)) + ":" + strconv.FormatInt(mill, 10)
		egtsMessageID, egtsRecID := s.ids()
		egts.PacketID = egtsMessageID
		egts.Data.(*nav.EgtsRecord).RecNum = egtsRecID
		packet, err := egts.Form()
		if err != nil {
			logger.Errorf("error forming egts: %s", err)
			return bufOld
		}
		bufOld = append(bufOld, packet...)
		logger.Debugf("writeEGTSid %d : %s", egtsMessageID, messageID)
		err = writeEGTSid(cR, egtsMessageID, messageID, logger)
		if err != nil {
			logger.Errorf("can't writeEGTSid: %v", err)
			cR = connRedis()
		}
	} else {
		logger.Errorf("parse NDTP error: %v", err)
	}
	return bufOld
}
