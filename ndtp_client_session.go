package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"net"
	"strconv"
	"sync"
	"time"
)

//session with NDTP client
func clientSession(client net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, s *session, mu *sync.Mutex) {
	logger := s.logger
	//connect to redis
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
	//close redis connection
	defer cR.Close()
	//remove ext message if it exists
	removeServerExt(cR, s)
	var restBuf []byte
	checkTicker := time.NewTicker(60 * time.Second)
	defer checkTicker.Stop()
	for {
		select {
		case <-checkTicker.C:
			go oldFromClient(cR, s, ndtpConn, mu, ErrNDTPCh)
		default:
			var b [defaultBufferSize]byte
			logger.Debug("start reading from client")
			client.SetReadDeadline(time.Now().Add(readTimeout))
			n, err := client.Read(b[:])
			logger.Debugf("received %d from client", n)
			printPacket(logger, "packet from client: ", b[:n])
			if err != nil {
				errClientCh <- err
				return
			}
			if enableMetrics {
				countFromServerNDTP.Inc(1)
			}
			restBuf = append(restBuf, b[:n]...)
			logger.Debugf("len(restBuf) = %d", len(restBuf))
			for {
				var data ndtpData
				var packet []byte
				logger.Debugf("before parsing len(restBuf) = %d", len(restBuf))
				logger.Debugf("before parsing restBuf: %v", restBuf)
				data, packet, restBuf, err = parseNDTP(restBuf)
				if err != nil {
					if len(restBuf) > defaultBufferSize {
						restBuf = []byte{}
					}
					logger.Debugf("error parseNDTP: %v", err)
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
					logger.Warningf("can't process message: %s", err)
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
	logger := s.logger
	controlReplyID, err := readControlID(cR, s, int(data.NPH.NPHReqID))
	logger.Debugf("old nphReqID: %d; new nphReqID: %d", data.NPH.NPHReqID, controlReplyID)
	printPacket(logger,"control message before changing: ", packet)
	message := changeContolResult(packet, controlReplyID, s)
	printPacket(logger,"control message after changing: ", message)
	ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = ndtpConn.conn.Write(message)
	if err != nil {
		logger.Warningf("can't send to NDTP server: %s", err)
	}
	return
}

func procNoNeedReply(cR redis.Conn, ndtpConn *connection, data *ndtpData, s *session, packet []byte, ErrNDTPCh chan error, mu *sync.Mutex) (err error) {
	logger := s.logger
	logger.Debugf("no need to reply on message servId: %d, type: %d", data.NPH.ServiceID, data.NPH.NPHType)
	packetCopyNDTP := make([]byte, len(packet))
	copy(packetCopyNDTP, packet)
	_, message := changePacket(packetCopyNDTP, s)
	printPacket(logger,"procNoNeedReply: send message to server (no reply): ", message)
	ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = ndtpConn.conn.Write(message)
	if err != nil {
		logger.Warningf("procNoNeedReply: send to NDTP server error (no reply): %s", err)
		ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
	}
	return
}

func handleExtClient(cR redis.Conn, client net.Conn, ndtpConn *connection, data *ndtpData, s *session, packet []byte, ErrNDTPCh, errClientCh chan error, mu *sync.Mutex, mill int64) (err error) {
	s.logger.Debugf("NPH_SRV_EXTERNAL_DEVICE type: %d, id: %d, packetNum: %d, res: %d", data.NPH.NPHType, data.ext.mesID, data.ext.packNum, data.ext.res)
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
	logger := s.logger
	packetCopy := copyPack(packet)
	err = writeExtClient(cR, s, mill, packet)
	if err != nil {
		logger.Errorf("handleExtTitleClient: send ext error reply to client because of: ", err)
		errorReplyExt(client, data.ext.mesID, data.ext.packNum, packetCopy, s)
		return
	}
	logger.Println("handleExtTitleClient: start to send ext device message to NDTP server")
	if ndtpConn.closed != true {
		packetCopyNDTP := copyPack(packet)
		_, message := changePacket(packetCopyNDTP, s)
		err = writeNDTPIdExt(cR, s, data.ext.mesID, mill)
		if err != nil {
			logger.Errorf("error writeNDTPIdExt: %v", err)
		} else {
			printPacket(logger,"handleExtTitleClient: send ext device message to server: ", message)
			ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			_, err = ndtpConn.conn.Write(message)
			if err != nil {
				logger.Warningf("can't send ext device message to NDTP server: %s", err)
				ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
			} else {
				if enableMetrics {
					countToServerNDTP.Inc(1)
				}
			}
		}
	}
	logger.Debugln("start to reply to ext device message")
	err = replyExt(client, data.ext.mesID, data.ext.packNum, packet, s)
	if err != nil {
		logger.Warningf("can't reply to ext device message: ", err)
		errClientCh <- err
	}
	return
}

func handleExtResClient(cR redis.Conn, ndtpConn *connection, data *ndtpData, s *session, packet []byte, mill int64) (err error) {
	logger := s.logger
	_, _, _, mesID, err := getServExt(cR, s)
	if err != nil {
		logger.Warningf("can't getServExt: %v", err)
	} else if mesID == uint64(data.ext.mesID) {
		if data.ext.res == 0 {
			logger.Debugf("received result and remove data from db")
			err = removeServerExt(cR, s)
			if err != nil {
				logger.Warningf("can't removeFromNDTPExt : %v", err)
			}
		} else {
			logger.Println("received result with error status")
			err = setFlagServerExt(cR, s, "1")
			if err != nil {
				logger.Warningf("can't setFlagServerExt: %v", err)
			}
		}
	} else {
		logger.Warningf("receive reply with mesID: %d; messID stored in DB: %d", data.ext.mesID, mesID)
	}
	packetCopy := copyPack(packet)
	_, message := changePacket(packetCopy, s)
	printPacket(logger,"send ext device message to server: ", message)
	ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = ndtpConn.conn.Write(message)
	if err != nil {
		logger.Warningf("can't write ext device result to server: %v", err)
		err = writeExtClient(cR, s, mill, packet)
		if err != nil {
			logger.Errorf("can't write2DB ext device result: %v", err)
		}
	}
	return
}

func procMes(cR redis.Conn, client net.Conn, ndtpConn *connection, data *ndtpData, s *session, packet []byte, ErrNDTPCh, errClientCh chan error, mu *sync.Mutex, mill int64) (err error) {
	logger := s.logger
	data.NPH.ID = uint32(s.id)
	packetCopy := make([]byte, len(packet))
	copy(packetCopy, packet)
	err = write2DB(cR, *data, s, packetCopy, mill)
	if err != nil {
		logger.Errorf("can't write2DB: ", err)
		errorReply(client, packetCopy, s)
		return
	}
	logger.Debugf("start to send to NDTP server")
	if ndtpConn.closed != true {
		packetCopyNDTP := make([]byte, len(packet))
		copy(packetCopyNDTP, packet)
		NPHReqID, message := changePacket(packetCopyNDTP, s)
		printPacket(logger,"packet after changing: ", message)
		err = writeNDTPid(cR, s, NPHReqID, mill)
		if err != nil {
			logger.Errorf("error writingNDTPid: %v", err)
		} else {
			ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			printPacket(logger,"send message to server: ", message)
			_, err = ndtpConn.conn.Write(message)
			if err != nil {
				logger.Warningf("can't send to NDTP server: %s", err)
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
			logger.Debugln("start to send to EGTS server")
			data.ToRnis.id = uint32(s.id)
			egtsCh <- data.ToRnis
		}
	}
	logger.Debugln("start to reply")
	packetCopyNDTP1 := make([]byte, len(packet))
	copy(packetCopyNDTP1, packet)
	err = reply(client, data.NPH, packetCopyNDTP1, s)
	if err != nil {
		logger.Warningf("procMes: error replying to att: ", err)
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

func replyExt(c net.Conn, mesID, packNum uint16, packet []byte, s *session) error {
	logger := s.logger
	logger.Debugf("packet: %v", packet)
	ans := answerExt(packet, mesID, packNum)
	logger.Debugf("length = %d; ans: %v", len(ans), ans)
	printPacket(logger,"send answer: ", ans)
	c.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := c.Write(ans)
	return err
}
func errorReplyExt(c net.Conn, mesID, packNum uint16, packet []byte, s *session) error {
	logger := s.logger
	logger.Debugf("packet: %v", packet)
	ans := errorAnswerExt(packet, mesID, packNum)
	logger.Debugf("length = %d; ans: %v", len(ans), ans)
	printPacket(logger,"send err reply: ", ans)
	c.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := c.Write(ans)
	return err
}