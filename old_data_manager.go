package main

import (
	"github.com/gomodule/redigo/redis"
	"log"
	"net"
	"sync"
	"time"
)

func checkOldDataNDTP(cR redis.Conn, s *session, ndtpConn *connection, mu *sync.Mutex, id int, ErrNDTPCh chan error) {
	res, err := getOldNDTP(cR, id)
	if err != nil {
		log.Println("can't get old NDTP for id: ", id)
		return
	}
	for _, mes := range res {
		var data ndtpData
		data, _, _, err = parseNDTP(mes)
		if ndtpConn.closed != true {
			mill, err := getScore(cR, id, mes)
			if err != nil {
				log.Printf("checkOldDataNDTP: error getting score for %v : %v", mes, err)
				continue
			}
			if data.NPH.ServiceID == NPH_SRV_EXTERNAL_DEVICE {
				log.Printf("checkOldDataNDTP: handle NPH_SRV_EXTERNAL_DEVICE type: %d, id: %d, packetNum: %d", data.NPH.NPHType, data.ext.mesID, data.ext.packNum)
				packetCopyNDTP := make([]byte, len(mes))
				copy(packetCopyNDTP, mes)
				_, message := changePacket(packetCopyNDTP, data, s)
				printPacket("checkOldDataNDTP: packet after changing ext device message: ", message)
				err = writeNDTPIdExt(cR, s.id, data.ext.mesID, data.ext.packNum, mill)
				if err != nil {
					log.Printf("error writeNDTPIdExt: %v", err)
				} else {
					ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
					printPacket("checkOldDataNDTP: send ext device message to server: ", message)
					_, err = ndtpConn.conn.Write(message)
					if err != nil {
						log.Printf("checkOldDataNDTP: send ext device message to NDTP server error: %s", err)
						ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
					} else {
						if enableMetrics {
							countToServerNDTP.Inc(1)
						}
					}
				}

			} else {
				var NPHReqID uint32
				var message []byte
				NPHReqID, message = changePacket(mes, data, s)
				err = writeNDTPid(cR, s.id, NPHReqID, mill)
				if err != nil {
					log.Printf("checkOldDataNDTP: error writeNDTPid: %v", err)
				} else {
					ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
					printPacket("checkOldDataNDTP: send message: ", message)
					_, err = ndtpConn.conn.Write(message)
					if err != nil {
						log.Printf("checkOldDataNDTP: send to NDTP server error: %s", err)
						ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
					} else {
						if enableMetrics {
							countToServerNDTP.Inc(1)
						}
					}
				}
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func checkOldDataNDTPServ(cR redis.Conn, s *session, client net.Conn, id int) {
	res, err := getOldNDTPServ(cR, id)
	if err != nil {
		log.Println("checkOldDataNDTPServ: can't get old NDTP for id: ", id)
		return
	}
	for _, mes := range res {
		var data ndtpData
		data, _, _, err = parseNDTP(mes)
		mill, err := getScoreServ(cR, id, mes)
		if err != nil {
			log.Printf("checkOldDataNDTPServ: error getting score for %v : %v", mes, err)
			continue
		}
		if data.NPH.ServiceID == NPH_SRV_EXTERNAL_DEVICE {
			log.Printf("checkOldDataNDTPServ: handle NPH_SRV_EXTERNAL_DEVICE type: %d, id: %d, packetNum: %d", data.NPH.NPHType, data.ext.mesID, data.ext.packNum)
			packetCopyNDTP := make([]byte, len(mes))
			copy(packetCopyNDTP, mes)
			_, message := changePacketFromServ(packetCopyNDTP, s)
			printPacket("checkOldDataNDTPServ: packet after changing ext device message: ", message)
			err = writeNDTPIdExtServ(cR, s.id, data.ext.mesID, data.ext.packNum, mill)
			if err != nil {
				log.Printf("checkOldDataNDTPServ: error writeNDTPIdExt: %v", err)
			} else {
				client.SetWriteDeadline(time.Now().Add(writeTimeout))
				printPacket("checkOldDataNDTPServ: send ext device message to server: ", message)
				_, err = client.Write(message)
				if err != nil {
					log.Printf("checkOldDataNDTPServ: send ext device message to NDTP server error: %s", err)
				}
			}
		} else {
			NPHReqID, message := changePacketFromServ(mes, s)
			err = writeNDTPidServ(cR, s.id, uint32(NPHReqID), mill)
			if err != nil {
				log.Printf("checkOldDataNDTPServ: error writeNDTPidServ: %v", err)
			} else {
				client.SetWriteDeadline(time.Now().Add(writeTimeout))
				printPacket("checkOldDataNDTPServ: send message: ", message)
				client.SetWriteDeadline(time.Now().Add(writeTimeout))
				_, err = client.Write(message)
				if err != nil {
					log.Printf("checkOldDataNDTPServ: send to NDTP server error: %s", err)
				}
			}
		}
		time.Sleep(1 * time.Millisecond)
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
		select {
		case <-ErrNDTPCh:
			return
		default:
			time.Sleep(1 * time.Hour)
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
