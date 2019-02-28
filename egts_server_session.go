package main

import (
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
	"time"
)

func egtsSession() {
	logger := logrus.WithField("egts", "new")
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			logger.Errorf("error connecting to redis: %s\n", err)
		} else {
			break
		}
	}
	defer cR.Close()
	var buf []byte
	var egtsMessageID, egtsRecID uint16
	var muOld sync.Mutex
	count := 0
	sendTicker := time.NewTicker(100 * time.Millisecond)
	checkTicker := time.NewTicker(60 * time.Second)
	defer sendTicker.Stop()
	defer checkTicker.Stop()
	for {
		select {
		case message := <-egtsCh:
			logger.Debugf("form egtsMessage: %d; egtsRecID: %d", egtsMessageID, egtsRecID)
			packet := formEGTS(message, egtsMessageID, egtsRecID)
			count += 1
			buf = append(buf, packet...)
			logger.Debugf("writeEGTSid in egtsSession: %d : %s", egtsRecID, message.messageID)
			printPacket(logger,"egts packet: ", packet)
			err := writeEGTSid(cR, egtsMessageID, message.messageID, logger)
			if err != nil {
				for {
					cR, err = redis.Dial("tcp", ":6379")
					if err != nil {
						logger.Errorf("error reconnecting to redis: %s\n", err)
					} else {
						break
					}
					time.Sleep(5 * time.Second)
				}
			}
			egtsMessageID++
			egtsRecID++
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
		case <-checkTicker.C:
			go oldEGTS(cR, &muOld, &egtsMessageID, &egtsRecID)
		}
	}
}

func waitReplyEGTS() {
	logger := logrus.WithField("egts", "new")
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			logger.Errorf("error connecting to redis: %s", err)
		} else {
			break
		}
	}
	defer cR.Close()
	for {
		var b [defaultBufferSize]byte
		if !egtsConn.closed {
			logger.Debugf("start reading data from EGTS server")
			egtsConn.conn.SetReadDeadline(time.Now().Add(readTimeout))
			n, err := egtsConn.conn.Read(b[:])
			printPacket(logger,"received packet: ", b[:n])
			if err != nil {
				logger.Warningf("can't get reply from egts server %s", err)
				go egtsConStatus()
				time.Sleep(5 * time.Second)
				continue
			}
			egtsMsgIDs, err := parseEGTS(b[:n])
			logger.Tracef("egtsMsgID: %v", egtsMsgIDs)
			if err != nil {
				logger.Errorf("error while parsing reply from EGTS %v: %s", b[:n], err)
			}
			for _, id := range egtsMsgIDs {
				err := deleteEGTS(cR, id, logger)
				if err != nil {
					logger.Warningf("can't deleteEGTS: %s", err)
					for {
						cR, err = redis.Dial("tcp", ":6379")
						if err != nil {
							logger.Errorf("error reconnecting to redis: %s", err)
						} else {
							break
						}
						time.Sleep(5 * time.Second)
					}
				}
			}
		} else {
			logger.Infoln("EGTS server closed")
			time.Sleep(5 * time.Second)
		}
	}
}

func send2egts(buf []byte, logger * logrus.Entry) {
	if !egtsConn.closed {
		egtsConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		printPacket(logger,"send2egts: sending packet: ", buf)
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

func reconnectEGTS(logger * logrus.Entry) {
	logger.Println("start reconnectEGTS")
	for {
		for i := 0; i < 3; i++ {
			logger.Printf("try to reconnect to EGTS server: %d", i)
			cE, err := net.Dial("tcp", EGTSAddress)
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
