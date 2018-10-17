package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

//session with NDTP client
func clientSession(client net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, s *session, mu *sync.Mutex) {
	//connect to redis
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			log.Printf("clientSession: error connecting to redis in clientSession: %s\n", err)
		} else {
			break
		}
	}
	//close redis connection
	defer cR.Close()
	var restBuf []byte
	checkTicker := time.NewTicker(60 * time.Second)
	defer checkTicker.Stop()
	for {
		select {
		case <-checkTicker.C:
			checkOldDataNDTP(cR, s, ndtpConn, mu, s.id, ErrNDTPCh)
		default:
			var b [defaultBufferSize]byte
			log.Println("clientSession: start reading from client")
			n, err := client.Read(b[:])
			log.Printf("clientSession: received %d from client", n)
			printPacket("clientSession: packet from client: ", b[:n])
			if err != nil {
				errClientCh <- err
				return
			}
			if enableMetrics {
				countFromServerNDTP.Inc(1)
			}
			restBuf = append(restBuf, b[:n]...)
			log.Printf("clientSession: len(restBuf) = %d", len(restBuf))
			for {
				var data ndtpData
				var packet []byte
				log.Printf("clientSession: before parsing len(restBuf) = %d", len(restBuf))
				log.Printf("clientSession: before parsing restBuf: %v", restBuf)
				data, packet, restBuf, err = parseNDTP(restBuf)
				if err != nil {
					if len(restBuf) > defaultBufferSize {
						restBuf = []byte{}
					}
					log.Printf("clientSession: error parseNDTP: %v", err)
					break
				}
				if enableMetrics {
					countClientNDTP.Inc(1)
				}
				mill := getMill()
				if data.NPH.isResult {
					err = procNPHResult(cR, ndtpConn, s, data.NPH.NPHReqID, packet, ErrNDTPCh, mu)
				} else if data.NPH.ServiceID == NPH_SRV_EXTERNAL_DEVICE {
					err = procExtDevice(cR, client, ndtpConn, data, s, packet, ErrNDTPCh, errClientCh, mu, mill)
				} else if !data.NPH.needReply {
					err = procNoNeedReply(cR, ndtpConn, data, s, packet, ErrNDTPCh, mu)
				} else {
					err = procMes(cR, client, ndtpConn, data, s, packet, ErrNDTPCh, errClientCh, mu, mill)
				}
				if err != nil {
					log.Printf("clientSession: error: %s", err)
					restBuf = []byte{}
					break
				}
				time.Sleep(1 * time.Millisecond)
				if len(restBuf) == 0 {
					break
				}

			}
		}
	}
}

func procNPHResult(cR redis.Conn, ndtpConn *connection, s *session, NPHReqID uint32, packet []byte, ErrNDTPCh chan error, mu *sync.Mutex) (err error) {
	log.Println("procNPHResult")
	err = removeFromNDTPServ(cR, s.id, NPHReqID)
	return
}

func procNoNeedReply(cR redis.Conn, ndtpConn *connection, data ndtpData, s *session, packet []byte, ErrNDTPCh chan error, mu *sync.Mutex) (err error) {
	log.Printf("procNoNeedReply: not need reply on message servId: %d, type: %d", data.NPH.ServiceID, data.NPH.NPHType)
	packetCopyNDTP := make([]byte, len(packet))
	copy(packetCopyNDTP, packet)
	_, message := changePacket(packetCopyNDTP, data, s)
	printPacket("procNoNeedReply: packet after changing (no reply): ", message)
	ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	printPacket("procNoNeedReply: send message to server (no reply): ", message)
	_, err = ndtpConn.conn.Write(message)
	if err != nil {
		log.Printf("procNoNeedReply: send to NDTP server error (no reply): %s", err)
		ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
	}
	return
}

func procExtDevice(cR redis.Conn, client net.Conn, ndtpConn *connection, data ndtpData, s *session, packet []byte, ErrNDTPCh, errClientCh chan error, mu *sync.Mutex, mill int64) (err error) {
	log.Printf("procExtDevice: handle NPH_SRV_EXTERNAL_DEVICE type: %d, id: %d, packetNum: %d", data.NPH.NPHType, data.ext.mesID, data.ext.packNum)
	if data.NPH.NPHType == NPH_SED_DEVICE_TITLE_DATA || data.NPH.NPHType == NPH_SED_DEVICE_DATA {
		data.NPH.ID = uint32(s.id)
		packetCopy := make([]byte, len(packet))
		copy(packetCopy, packet)
		err = write2DB(cR, data, s, packetCopy, mill)
		if err != nil {
			log.Println("procExtDevice: send ext error reply to client because of: ", err)
			errorReplyExt(client, data.ext.mesID, data.ext.packNum, packetCopy)
			return
		}
		log.Println("procExtDevice: start to send ext device message to NDTP server")
		if ndtpConn.closed != true {
			packetCopyNDTP := make([]byte, len(packet))
			copy(packetCopyNDTP, packet)
			_, message := changePacket(packetCopyNDTP, data, s)
			printPacket("procExtDevice: packet after changing ext device message: ", message)
			err = writeNDTPIdExt(cR, s.id, data.ext.mesID, data.ext.packNum, mill)
			if err != nil {
				log.Printf("error writeNDTPIdExt: %v", err)
			} else {
				ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				printPacket("procExtDevice: send ext device message to server: ", message)
				_, err = ndtpConn.conn.Write(message)
				if err != nil {
					log.Printf("procExtDevice: send ext device message to NDTP server error: %s", err)
					ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
				} else {
					if enableMetrics {
						countToServerNDTP.Inc(1)
					}
				}
			}
		}
		log.Println("procExtDevice: start to reply to ext device message")
		err = replyExt(client, data.ext.mesID, data.ext.packNum, packet)
		if err != nil {
			log.Println("procExtDevice: error replying to ext device message: ", err)
			errClientCh <- err
			return
		}
	} else {
		if data.NPH.NPHType == NPH_SED_DEVICE_RESULT {
			err = removeFromNDTPExtServ(cR, s.id, data.ext.mesID, data.ext.packNum)
			if err != nil {
				log.Printf("procExtDevice: removeFromNDTPExt error for id %d : %v", s.id, err)
			}
		} else {
			err = fmt.Errorf("handle NPH_SRV_EXTERNAL_DEVICE type: %d, id: %d, packetNum: %d", data.NPH.NPHType, data.ext.mesID, data.ext.packNum)
			fmt.Printf("procExtDevice: error %s", err)
		}
	}
	return
}

func procMes(cR redis.Conn, client net.Conn, ndtpConn *connection, data ndtpData, s *session, packet []byte, ErrNDTPCh, errClientCh chan error, mu *sync.Mutex, mill int64) (err error) {
	data.NPH.ID = uint32(s.id)
	packetCopy := make([]byte, len(packet))
	copy(packetCopy, packet)
	err = write2DB(cR, data, s, packetCopy, mill)
	if err != nil {
		log.Println("procMes: send error reply to server because of: ", err)
		errorReply(client, packetCopy)
		return
	}
	log.Println("procMes: start to send to NDTP server")
	if ndtpConn.closed != true {
		packetCopyNDTP := make([]byte, len(packet))
		copy(packetCopyNDTP, packet)
		NPHReqID, message := changePacket(packetCopyNDTP, data, s)
		printPacket("procMes: packet after changing: ", message)
		err = writeNDTPid(cR, s.id, NPHReqID, mill)
		if err != nil {
			log.Printf("error writingNDTPid: %v", err)
		} else {
			ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			printPacket("procMes: send message to server: ", message)
			_, err = ndtpConn.conn.Write(message)
			if err != nil {
				log.Printf("procMes: send to NDTP server error: %s", err)
				ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
			} else {
				if enableMetrics {
					countToServerNDTP.Inc(1)
				}
			}
		}
	}
	data.ToRnis.messageID = strconv.Itoa(s.id) + ":" + strconv.FormatInt(mill, 10)
	if egtsConn.closed != true {
		if toEGTS(data) {
			log.Println("procMes: start to send to EGTS server")
			data.ToRnis.id = uint32(s.id)
			egtsCh <- data.ToRnis
		}
	}
	log.Println("procMes: start to reply")
	err = reply(client, data.NPH, packet)
	if err != nil {
		log.Println("procMes: error replying to att: ", err)
		errClientCh <- err
		return
	}
	return
}

func toEGTS(data ndtpData) bool {
	if data.ToRnis.time != 0 {
		return true
	}
	return false
}