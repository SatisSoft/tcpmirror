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
	//remove ext message if it exists
	removeServerExt(cR, s.id)
	var restBuf []byte
	checkTicker := time.NewTicker(60 * time.Second)
	defer checkTicker.Stop()
	for {
		select {
		case <-checkTicker.C:
			checkOldDataClient(cR, s, ndtpConn, mu, s.id, ErrNDTPCh)
		default:
			var b [defaultBufferSize]byte
			log.Println("clientSession: start reading from client")
			client.SetReadDeadline(time.Now().Add(readTimeout))
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
					err = procNPHResult(cR, ndtpConn, s, &data, packet)
				} else if data.NPH.ServiceID == NPH_SRV_EXTERNAL_DEVICE {
					err = handleExtClient(cR, client, ndtpConn, &data, s, packet, ErrNDTPCh, errClientCh, mu, mill)
				} else if !data.NPH.needReply {
					err = procNoNeedReply(cR, ndtpConn, &data, s, packet, ErrNDTPCh, mu)
				} else {
					err = procMes(cR, client, ndtpConn, &data, s, packet, ErrNDTPCh, errClientCh, mu, mill)
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

func procNPHResult(cR redis.Conn, ndtpConn *connection, s *session, data *ndtpData, packet []byte) (err error) {
	controlReplyID, err := readControlID(cR, s.id, int(data.NPH.NPHReqID))
	log.Printf("procNPHResult: old nphReqID: %d; new nphReqID: %d", data.NPH.NPHReqID, controlReplyID)
	printPacket("procNPHResult: control message before changing: ", packet)
	message := changeContolResult(packet, controlReplyID, s)
	printPacket("procNPHResult: control message after changing: ", message)
	ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = ndtpConn.conn.Write(message)
	if err != nil {
		log.Printf("procNPHResult: send to NDTP server error: %s", err)
	}
	return
}

func procNoNeedReply(cR redis.Conn, ndtpConn *connection, data *ndtpData, s *session, packet []byte, ErrNDTPCh chan error, mu *sync.Mutex) (err error) {
	log.Printf("procNoNeedReply: not need reply on message servId: %d, type: %d", data.NPH.ServiceID, data.NPH.NPHType)
	packetCopyNDTP := make([]byte, len(packet))
	copy(packetCopyNDTP, packet)
	_, message := changePacket(packetCopyNDTP, s)
	printPacket("procNoNeedReply: send message to server (no reply): ", message)
	ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = ndtpConn.conn.Write(message)
	if err != nil {
		log.Printf("procNoNeedReply: send to NDTP server error (no reply): %s", err)
		ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
	}
	return
}

func handleExtClient(cR redis.Conn, client net.Conn, ndtpConn *connection, data *ndtpData, s *session, packet []byte, ErrNDTPCh, errClientCh chan error, mu *sync.Mutex, mill int64) (err error) {
	log.Printf("handleExtClient: handle NPH_SRV_EXTERNAL_DEVICE type: %d, id: %d, packetNum: %d, res: %d", data.NPH.NPHType, data.ext.mesID, data.ext.packNum, data.ext.res)
	if data.NPH.NPHType == NPH_SED_DEVICE_TITLE_DATA {
		err = handleExtTitleClient(cR, client, ndtpConn, data, s, packet, ErrNDTPCh, errClientCh, mu, mill)
	} else if data.NPH.NPHType == NPH_SED_DEVICE_RESULT {
		err = handleExtResClient(cR, ndtpConn, data, s, packet, mill)
	} else {
		err = fmt.Errorf("unknown NPHType: %d, packet %v", data.NPH.NPHType, packet)
	}
	return
}

func handleExtTitleClient(cR redis.Conn, client net.Conn, ndtpConn *connection, data *ndtpData, s *session, packet []byte, ErrNDTPCh, errClientCh chan error, mu *sync.Mutex, mill int64) (err error) {
	packetCopy := copyPack(packet)
	err = writeExtClient(cR, s.id, mill, packet)
	if err != nil {
		log.Println("handleExtTitleClient: send ext error reply to client because of: ", err)
		errorReplyExt(client, data.ext.mesID, data.ext.packNum, packetCopy)
		return
	}
	log.Println("handleExtTitleClient: start to send ext device message to NDTP server")
	if ndtpConn.closed != true {
		packetCopyNDTP := copyPack(packet)
		_, message := changePacket(packetCopyNDTP, s)
		err = writeNDTPIdExt(cR, s.id, data.ext.mesID, mill)
		if err != nil {
			log.Printf("error writeNDTPIdExt: %v", err)
		} else {
			printPacket("handleExtTitleClient: send ext device message to server: ", message)
			ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			_, err = ndtpConn.conn.Write(message)
			if err != nil {
				log.Printf("handleExtTitleClient: send ext device message to NDTP server error: %s", err)
				ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
			} else {
				if enableMetrics {
					countToServerNDTP.Inc(1)
				}
			}
		}
	}
	log.Println("handleExtTitleClient: start to reply to ext device message")
	err = replyExt(client, data.ext.mesID, data.ext.packNum, packet)
	if err != nil {
		log.Println("handleExtTitleClient: error replying to ext device message: ", err)
		errClientCh <- err
	}
	return
}

func handleExtResClient(cR redis.Conn, ndtpConn *connection, data *ndtpData, s *session, packet []byte, mill int64) (err error) {
	_, _, _, mesID, err := getServExt(cR, s.id)
	if err != nil {
		log.Printf("handleExtResClient: error getServExt: %v", err)
	} else if mesID == uint64(data.ext.mesID) {
		if data.ext.res == 0 {
			log.Println("handleExtResClient: received result and remove data from db")
			err = removeServerExt(cR, s.id)
			if err != nil {
				log.Printf("handleExtResClient: removeFromNDTPExt error for id %d : %v", s.id, err)
			}
		} else {
			log.Println("handleExtResClient: received result with error status")
			err = setFlagServerExt(cR, s.id, "1")
			if err != nil {
				log.Printf("handleExtResClient: setFlagServerExt error: %v", err)
			}
		}
	} else {
		log.Printf("handleExtResClient: receive reply with mesID: %d; messID stored in DB: %d", data.ext.mesID, mesID)
	}
	packetCopy := copyPack(packet)
	_, message := changePacket(packetCopy, s)
	printPacket("handleExtResClient: send ext device message to server: ", message)
	ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = ndtpConn.conn.Write(message)
	if err != nil {
		log.Printf("handleExtResClient: error write ext device result to server: %v", err)
		err = writeExtClient(cR, s.id, mill, packet)
		if err != nil {
			log.Printf("handleExtResClient: error write2DB ext device result: %v", err)
		}
	}
	return
}

func procMes(cR redis.Conn, client net.Conn, ndtpConn *connection, data *ndtpData, s *session, packet []byte, ErrNDTPCh, errClientCh chan error, mu *sync.Mutex, mill int64) (err error) {
	data.NPH.ID = uint32(s.id)
	packetCopy := make([]byte, len(packet))
	copy(packetCopy, packet)
	err = write2DB(cR, *data, s, packetCopy, mill)
	if err != nil {
		log.Println("procMes: send error reply to server because of: ", err)
		errorReply(client, packetCopy)
		return
	}
	log.Println("procMes: start to send to NDTP server")
	if ndtpConn.closed != true {
		packetCopyNDTP := make([]byte, len(packet))
		copy(packetCopyNDTP, packet)
		NPHReqID, message := changePacket(packetCopyNDTP, s)
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
	packetCopyNDTP1 := make([]byte, len(packet))
	copy(packetCopyNDTP1, packet)
	err = reply(client, data.NPH, packetCopyNDTP1)
	if err != nil {
		log.Println("procMes: error replying to att: ", err)
		errClientCh <- err
		return
	}
	return
}

func toEGTS(data *ndtpData) bool {
	if data.ToRnis.time != 0 {
		return true
	}
	return false
}
