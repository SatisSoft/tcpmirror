package main

import (
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"net"
	"time"
)

import nav "github.com/ashirko/navprot"

func egtsServerSession() {
	logger := logrus.WithField("egts", "new")
	s := new(egtsSession)
	egtsConn = connection{egtsServer, nil, true, false}
	cE, err := net.Dial("tcp", egtsServer)
	if err != nil {
		logrus.Errorf("error while connecting to EGTS server: %s", err)
	} else {
		egtsConn.conn = cE
		egtsConn.closed = false
	}
	cR := connRedis()
	defer cR.Close()
	var buf []byte
	count := 0
	sendTicker := time.NewTicker(100 * time.Millisecond)
	defer sendTicker.Stop()
	go oldEGTS(s)
	go waitReplyEGTS()
	go egtsRemoveExpired()
	for {
		select {
		case message := <-egtsCh:
			buf = processMessage(s, cR, message, buf, logger)
			count++
			if count == 10 {
				send2egts(buf, logger)
				if enableMetrics {
					countServerEGTS.Inc(10)
				}
				count = 0
				buf = nil
			}
		case <-sendTicker.C:
			if (count > 0) && (count < 10) {
				send2egts(buf, logger)
				if enableMetrics {
					countServerEGTS.Inc(int64(count))
				}
				count = 0
				buf = nil
			}
		}
	}
}

func processMessage(s *egtsSession, cR redis.Conn, message *egtsMsg, buf []byte, logger *logrus.Entry) []byte {
	egts := message.msg
	egtsMessageID, egtsRecID := s.ids()
	egts.PacketID = egtsMessageID
	egts.Data.(*nav.EgtsRecord).RecNum = egtsRecID
	packet, err := egts.Form()
	if err != nil {
		logger.Errorf("error forming egts: %s", err)
		return buf
	}
	buf = append(buf, packet...)
	logger.Debugf("writeEGTSid in egtsServerSession: %d : %s", egtsRecID, message.msgID)
	printPacket(logger, "egts packet: ", packet)
	err = writeEGTSid(cR, egtsMessageID, message.msgID, logger)
	if err != nil {
		logger.Errorf("error wrinteEGTSid: %s", err)
		cR = connRedis()
	}
	return buf
}

func waitReplyEGTS() {
	logger := logrus.WithField("egts", "waitReply")
	var restBuf []byte
	cR := connRedis()
	defer cR.Close()
	for {
		var b [defaultBufferSize]byte
		if !egtsConn.closed {
			logger.Debugf("start reading data from EGTS server")
			egtsConn.conn.SetReadDeadline(time.Now().Add(readTimeout))
			n, err := egtsConn.conn.Read(b[:])
			printPacket(logger, "received packet: ", b[:n])
			if err != nil {
				logger.Warningf("can't get reply from egts server %s", err)
				go egtsConStatus()
				time.Sleep(5 * time.Second)
				continue
			}
			restBuf = append(restBuf, b[:n]...)
			for {
				egts := new(nav.EGTS)
				restBuf, err = egts.Parse(restBuf)
				if err != nil {
					logger.Errorf("error while parsing reply from EGTS %v: %s", restBuf, err)
					restBuf = []byte{}
					break

				}
				data := egts.Data.(*nav.EgtsResponce)
				if data.ProcRes != 0 {
					logger.Warningf("received egts responce with not zero result: %d", data.ProcRes)
					continue
				}
				err := deleteEGTS(cR, data.RecID, logger)
				if err != nil {
					logger.Warningf("can't deleteEGTS: %s", err)
					cR = connRedis()
				}
				if len(restBuf) == 0 {
					break
				}
			}
		} else {
			logger.Warningf("EGTS server closed")
			time.Sleep(5 * time.Second)
		}
	}
}

func connRedis() redis.Conn {
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			logrus.Errorf("error connecting to redis: %s\n", err)
		} else {
			break
		}
		time.Sleep(5 * time.Second)
	}
	return cR
}

func send2egts(buf []byte, logger *logrus.Entry) {
	if !egtsConn.closed {
		printPacket(logger, "send2egts: sending packet: ", buf)
		egtsConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		_, err := egtsConn.conn.Write(buf)
		if err != nil {
			egtsConStatus()
		}
	}
}

func egtsConStatus() {
	logger := logrus.WithField("egts", "reconnect")
	logger.Println("start egtsConStatus")
	egtsMu.Lock()
	defer egtsMu.Unlock()
	logger.Printf("closed: %t; recon: %t", egtsConn.closed, egtsConn.recon)
	if egtsConn.closed || egtsConn.recon {
		return
	} else {
		egtsConn.recon = true
		egtsConn.conn.Close()
		egtsConn.closed = true
		go reconnectEGTS(logger)
	}
}

func reconnectEGTS(logger *logrus.Entry) {
	logger.Println("start reconnectEGTS")
	for {
		for i := 0; i < 3; i++ {
			logger.Printf("try to reconnect to EGTS server: %d", i)
			cE, err := net.Dial("tcp", egtsServer)
			if err == nil {
				egtsConn.conn = cE
				egtsConn.closed = false
				logger.Printf("reconnected to EGTS server")
				time.Sleep(1 * time.Minute)
				egtsConn.recon = false
				return
			}
			logger.Warningf("error while reconnecting to EGTS server: %s", err)
		}
		time.Sleep(10 * time.Second)
	}
}
