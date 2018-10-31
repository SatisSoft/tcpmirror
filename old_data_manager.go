package main

import (
	"github.com/gomodule/redigo/redis"
	"log"
	"net"
	"sync"
	"time"
)

func checkOldDataClient(cR redis.Conn, s *session, ndtpConn *connection, mu *sync.Mutex, id int, ErrNDTPCh chan error) {
	res, err := getOldNDTP(cR, id)
	if err != nil {
		log.Printf("checkOldDataNDTP: getOldNDTP error for if %d : %v", id, err)
	} else {
		resendNav(cR, s, ndtpConn, mu, id, ErrNDTPCh, res)
	}
	res, err = getOldNDTPExt(cR, id)
	if err != nil {
		log.Printf("checkOldDataNDTP: getOldNDTPExt error for if %d : %v", id, err)
	} else {
		resendExt(cR, s, ndtpConn, mu, id, ErrNDTPCh, res)
	}
}

func resendNav(cR redis.Conn, s *session, ndtpConn *connection, mu *sync.Mutex, id int, ErrNDTPCh chan error, res [][]byte) {
	for _, mes := range res {
		data, _, _, _ := parseNDTP(mes)
		if ndtpConn.closed != true {
			mill, err := getScore(cR, id, mes)
			if err != nil {
				log.Printf("resendNav: error getting score for %v : %v", mes, err)
				continue
			}
			NPHReqID, message := changePacketHistory(mes, data, s)
			err = writeNDTPid(cR, s.id, NPHReqID, mill)
			if err != nil {
				log.Printf("resendNav: error writeNDTPid: %v", err)
			} else {
				ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				printPacket("resendNav: send message: ", message)
				_, err = ndtpConn.conn.Write(message)
				if err != nil {
					log.Printf("resendNav: send to NDTP server error: %s", err)
					ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
				} else if enableMetrics {
					countToServerNDTP.Inc(1)
				}
			}
		} else {
			log.Printf("resendNav: connection to server closed")
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func resendExt(cR redis.Conn, s *session, ndtpConn *connection, mu *sync.Mutex, id int, ErrNDTPCh chan error, res [][]byte) {
	for _, mes := range res {
		var data ndtpData
		data, _, _, _ = parseNDTP(mes)
		if ndtpConn.closed != true {
			mill, err := getScoreExt(cR, id, mes)
			if err != nil {
				log.Printf("resendExt: error getting score for ext %v : %v", mes, err)
				continue
			}
			if data.NPH.NPHType == NPH_SED_DEVICE_RESULT {
				_, message := changePacket(mes, s)
				printPacket("resendExt: send message: ", message)
				ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				_, err = ndtpConn.conn.Write(message)
				if err != nil {
					log.Printf("resendExt: send to NDTP server error: %s", err)
					ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
				} else {
					log.Printf("resendExt: remove old data for id: %d", id)
					removeOldExt(cR, id, mill)
					if enableMetrics {
						countToServerNDTP.Inc(1)
					}
				}
			} else if data.NPH.NPHType == NPH_SED_DEVICE_TITLE_DATA {
				_, message := changePacket(mes, s)
				printPacket("resendExt: packet after changing ext device message: ", message)
				err = writeNDTPIdExt(cR, s.id, data.ext.mesID, mill)
				if err != nil {
					log.Printf("error writeNDTPIdExt: %v", err)
				} else {
					printPacket("resendExt: send ext device message to server: ", message)
					ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
					_, err = ndtpConn.conn.Write(message)
					if err != nil {
						log.Printf("resendExt: send ext device message to NDTP server error: %s", err)
						ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
					} else if enableMetrics {
						countToServerNDTP.Inc(1)
					}
				}
			}
		} else {
			log.Printf("resendExt: connection to server closed")
			return
		}
	}
}

func checkOldDataServ(cR redis.Conn, s *session, client net.Conn, id int) {
	res, mill, flag, _, err := getServExt(cR, id)
	if err != nil {
		log.Printf("checkOldDataNDTPServ: error getServExt: %v", err)
		return
	}
	log.Printf("checkOldDataNDTPServ: id: %d; mill: %d; flag: %s; res: %v", s.id, mill, flag, res)
	now := getMill()
	if now-mill > 60000 && flag == "0" {
		err = removeServerExt(cR, s.id)
		if err != nil {
			log.Printf("checkOldDataNDTPServ: error removing old ext data for id %d : %v", s.id, err)
		}
		return
	}
	err = setFlagServerExt(cR, s.id, "0")
	if err != nil {
		log.Printf("checkOldDataNDTPServ: setNDTPExtFlag error: %s", err)
	}
	_, message := changePacketFromServ(res, s)
	printPacket("checkOldDataNDTPServ: send ext device message to client: ", message)
	client.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = client.Write(message)
	if err != nil {
		log.Printf("checkOldDataNDTPServ: send ext device message to NDTP client error: %s", err)
	}
}

func ndtpRemoveExpired(id int, ErrNDTPCh chan error) {
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			log.Printf("error connecting to redis in ndtpRemoveExpired for id %d: %s\n", id, err)
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
		err := removeExpiredDataNDTP(cR, id)
		if err != nil {
			log.Printf("error while remove expired data ndtp for id %d: %s", id, err)
			for {
				cR, err = redis.Dial("tcp", ":6379")
				if err != nil {
					log.Printf("error reconnecting to redis in ndtpRemoveExpired for id %d: %s\n", id, err)
				} else {
					break
				}
				time.Sleep(1 * time.Minute)
			}
		}
	}
}

func egtsRemoveExpired() {
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			log.Printf("error connecting to redis in egtsRemoveExpired: %s\n", err)
		} else {
			break
		}
	}
	defer cR.Close()
	for {
		err := removeExpiredDataEGTS(cR)
		if err != nil {
			log.Printf("error while remove expired data EGTS %s", err)
			for {
				cR, err = redis.Dial("tcp", ":6379")
				if err != nil {
					log.Printf("error reconnecting to redis in egtsRemoveExpired: %s\n", err)
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
	log.Println("checkOldDataEGTS: start")
	messages, err := getOldEGTS(cR)
	if err != nil {
		log.Printf("can't get old EGTS %s", err)
		return
	}
	var bufOld []byte
	var i int
	for _, msg := range messages {
		if i < 10 {
			log.Printf("checkOldDataEGTS: try to send message %v", msg)
			var dataNDTP ndtpData
			dataNDTP, _, _, err = parseNDTP(msg)
			if err != nil {
				packet := formEGTS(dataNDTP.ToRnis, *egtsMessageID, *egtsReqID)
				bufOld = append(bufOld, packet...)
				log.Printf("writeEGTSid in checkOldDataEGTS: %d : %s", *egtsMessageID, dataNDTP.ToRnis.messageID)
				err := writeEGTSid(cR, *egtsMessageID, dataNDTP.ToRnis.messageID)
				if err != nil {
					log.Printf("error writeEGTSid in checkOldDataEGTS: %v", err)
					continue
				}
				*egtsMessageID++
				*egtsReqID++
				if err != nil {
					log.Printf("checkOldDataEGTS: error while write EGTS id %s: %s", dataNDTP.ToRnis.messageID, err)
				}
			}
			i++
		} else {
			i = 0
			send2egts(bufOld)
			bufOld = nil
		}
	}
}
