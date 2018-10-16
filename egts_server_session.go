package main

import (
	"github.com/gomodule/redigo/redis"
	"log"
	"net"
	"time"
)

func egtsSession() {
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			log.Printf("egtsSession: error connecting to redis in egtsSession: %s\n", err)
		} else {
			break
		}
	}
	defer cR.Close()
	var buf []byte
	var egtsMessageID, egtsRecID uint16
	count := 0
	sendTicker := time.NewTicker(100 * time.Millisecond)
	checkTicker := time.NewTicker(60 * time.Second)
	defer sendTicker.Stop()
	defer checkTicker.Stop()
	for {
		select {
		case message := <-egtsCh:
			log.Printf("egtsSession: form egtsMessage: %d; egtsRecID: %d", egtsMessageID, egtsRecID)
			packet := formEGTS(message, egtsMessageID, egtsRecID)
			count += 1
			buf = append(buf, packet...)
			log.Printf("egtsSession: writeEGTSid in egtsSession: %d : %s", egtsRecID, message.messageID)
			printPacket("egtsSession: egts packet: ", packet)
			err := writeEGTSid(cR, egtsMessageID, message.messageID)
			if err != nil {
				for {
					cR, err = redis.Dial("tcp", ":6379")
					if err != nil {
						log.Printf("egtsSession: error reconnecting to redis in egtsSession 1: %s\n", err)
					} else {
						break
					}
					time.Sleep(5 * time.Second)
				}
			}
			egtsMessageID++
			egtsRecID++
			if err != nil {
				log.Printf("egtsSession: error while write EGTS id in egtsSession %s: %s", message.messageID, err)
			} else if count == 10 {
				send2egts(buf)
				if enableMetrics {
					countServerEGTS.Inc(10)
				}
				count = 0
				buf = nil
			}
		case <-sendTicker.C:
			if (count > 0) && (count < 10) {
				send2egts(buf)
				if enableMetrics {
					countServerEGTS.Inc(int64(count))
				}
				count = 0
				buf = nil
			}
		case <-checkTicker.C:
			checkOldDataEGTS(cR, &egtsMessageID, &egtsRecID)
		}
	}
}

func waitReplyEGTS() {
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			log.Printf("waitReplyEGTS: error connecting to redis in waitReplyEGTS: %s\n", err)
		} else {
			break
		}
	}
	defer cR.Close()
	for {
		var b [defaultBufferSize]byte
		if !egtsConn.closed {
			log.Println("waitReplyEGTS: start reading data from EGTS server")
			//egtsConn.conn.SetReadDeadline(time.Now().Add(writeTimeout))
			n, err := egtsConn.conn.Read(b[:])
			log.Printf("waitReplyEGTS: received %d bytes; packet: %v", n, b[:n])
			if err != nil {
				log.Printf("waitReplyEGTS: error while getting reply from egts server %s", err)
				go egtsConStatus()
				time.Sleep(5 * time.Second)
				continue
			}
			egtsMsgIDs, err := parseEGTS(b[:n])
			if err != nil {
				log.Printf("waitReplyEGTS: error while parsing reply from EGTS %v: %s", b[:n], err)
			}
			for _, id := range egtsMsgIDs {
				err := deleteEGTS(cR, id)
				if err != nil {
					log.Printf("waitReplyEGTS: error while delete EGTS id %s", err)
					for {
						cR, err = redis.Dial("tcp", ":6379")
						if err != nil {
							log.Printf("waitReplyEGTS: error reconnecting to redis in waitReplyEGTS: %s\n", err)
						} else {
							break
						}
						time.Sleep(5 * time.Second)
					}
				}
			}
		} else {
			log.Println("EGTS server closed")
			time.Sleep(5 * time.Second)
		}
	}
}

func send2egts(buf []byte) {
	if !egtsConn.closed {
		egtsConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		printPacket("send2egts: sending packet: ", buf)
		_, err := egtsConn.conn.Write(buf)
		if err != nil {
			egtsConStatus()
		}
	}
}

func reconnectEGTS() {
	log.Println("start reconnectEGTS")
	for {
		for i := 0; i < 3; i++ {
			log.Printf("try to reconnect to EGTS server: %d", i)
			cE, err := net.Dial("tcp", EGTSAddress)
			if err == nil {
				egtsConn.conn = cE
				egtsConn.closed = false
				log.Printf("reconnected to EGTS server")
				time.Sleep(1 * time.Minute)
				egtsConn.recon = false
				return
			}
			log.Printf("error while reconnecting to EGTS server: %s", err)
		}
		time.Sleep(10 * time.Second)
	}
}
