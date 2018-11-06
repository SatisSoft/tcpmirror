package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"net"
	"sync"
	"time"
)

//session with NDTP server
func serverSession(client net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, s *session, mu *sync.Mutex) {
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
	var restBuf []byte
	//check old data every 60 seconds
	checkTicker := time.NewTicker(60 * time.Second)
	defer checkTicker.Stop()
	//receive, process and send data to a client
	for {
		select {
		case <-checkTicker.C:
			checkOldDataServ(cR, s, client, s.id)
		default:
			//check if connection with client is closed
			if conClosed(ErrNDTPCh) {
				return
			}
			//if connection to client is not closed, reading data from server
			if !ndtpConn.closed {
				var b [defaultBufferSize]byte
				ndtpConn.conn.SetReadDeadline(time.Now().Add(readTimeout))
				n, err := ndtpConn.conn.Read(b[:])
				logger.Debugf("ndtpConn.closed = %t; ndtpConn.recon = %t", ndtpConn.closed, ndtpConn.recon)
				logger.Debugf("received %d bytes from server", n)
				printPacket(logger,"packet from server: ", b[:n])
				if err != nil {
					logger.Warningf("can't get data from server: %v", err)
					ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
					time.Sleep(5 * time.Second)
					continue
				}
				restBuf = append(restBuf, b[:n]...)
				logger.Debugf("len(restBuf) = %d", len(restBuf))
				for {
					var data ndtpData
					var packet []byte
					data, packet, restBuf, err = parseNDTP(restBuf)
					if err != nil {
						logger.Warningf(" error while parsing NDTP from server: %v", err)
						restBuf = []byte{}
						break
					}
					if !data.valid {
						restBuf = nil
						continue
					}
					if data.NPH.isResult {
						err = handleNPHResult(cR, s, &data)
					} else if data.NPH.ServiceID == NPH_SRV_EXTERNAL_DEVICE {
						err = handleExtServ(cR, client, errClientCh, s, &data, packet)
					} else {
						err = handlePacket(cR, client, errClientCh, s, &data, packet)
					}
					if err != nil {
						logger.Warningf("error: %s", err)
						restBuf = []byte{}
						break
					}
					time.Sleep(1 * time.Millisecond)
					if len(restBuf) == 0 {
						break
					}
				}
			} else {
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func handleNPHResult(cR redis.Conn, s *session, data *ndtpData) (err error) {
	if data.NPH.NPHResult == 0 {
		err = removeFromNDTP(cR, s.id, data.NPH.NPHReqID)
	} else {
		s.logger.Warningf("nph result error: %d", data.NPH.NPHResult)
	}
	return
}

func handlePacket(cR redis.Conn, client net.Conn, errClientCh chan error, s *session, data *ndtpData, packet []byte) (err error) {
	logger := s.logger
	printPacket(logger,"before changing control message: ", packet)
	reqID, message := changePacketFromServ(packet, s)
	logger.Debugf("old nphReqID: %d; new nphReqID: %d", data.NPH.NPHReqID, reqID)
	writeControlID(cR, s.id, reqID, data.NPH.NPHReqID)
	printPacket(logger,"send control message to client: ", message)
	client.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = client.Write(message)
	if err != nil {
		errClientCh <- err
		return
	}
	return
}

func handleExtServ(cR redis.Conn, client net.Conn, errClientCh chan error, s *session, data *ndtpData, packet []byte) (err error) {
	logger := s.logger
	logger.Debugf("NPH_SRV_EXTERNAL_DEVICE type: %d, id: %d, packetNum: %d, res: %d", data.NPH.NPHType, data.ext.mesID, data.ext.packNum, data.ext.res)
	if data.NPH.NPHType == NPH_SED_DEVICE_TITLE_DATA {
		err = handleExtTitleServ(cR, client, errClientCh, s, data, packet)
	} else if data.NPH.NPHType == NPH_SED_DEVICE_RESULT {
		err = handleExtResServ(cR, s, data)
	} else {
		err = fmt.Errorf("unknown NPHType: %d, packet %v", data.NPH.NPHType, packet)
	}
	return err
}

func handleExtTitleServ(cR redis.Conn, client net.Conn, errClientCh chan error, s *session, data *ndtpData, packet []byte) (err error) {
	logger := s.logger
	packetCopy := copyPack(packet)
	mill := getMill()
	err = writeExtServ(cR, s.id, packetCopy, mill, data.ext.mesID)
	if err != nil {
		logger.Errorf("can't writeExtServ: %s", err)
		return
	}
	logger.Debugf("start to send ext device message to NDTP server")
	packetCopy1 := copyPack(packet)
	_, message := changePacketFromServ(packetCopy1, s)
	printPacket(logger,"send ext device message to client: ", message)
	client.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = client.Write(message)
	if err != nil {
		logger.Warningf("can't send ext device message to NDTP server: %s", err)
		errClientCh <- err
	}
	return
}

func handleExtResServ(cR redis.Conn, s *session, data *ndtpData) (err error) {
	logger := s.logger
	if data.ext.res == 0 {
		logger.Debugln("received result and remove data from db")
		err = removeFromNDTPExt(cR, s.id, data.ext.mesID)
		if err != nil {
			logger.Errorf("can't removeFromNDTPExt: %v", err)
		}
	} else {
		err = fmt.Errorf("received ext result message with error status for id %d: %d", s.id, data.ext.res)
		logger.Warningf("received result with failed status: %d", data.ext.res)
	}
	return
}

func conClosed(ErrNDTPCh chan error) bool {
	select {
	case <-ErrNDTPCh:
		return true
	default:
		return false
	}
}

func ndtpConStatus(cR redis.Conn, ndtpConn *connection, s *session, mu *sync.Mutex, ErrNDTPCh chan error) {
	mu.Lock()
	defer mu.Unlock()
	if ndtpConn.closed || ndtpConn.recon {
		return
	} else {
		ndtpConn.recon = true
		ndtpConn.conn.Close()
		ndtpConn.closed = true
		go reconnectNDTP(cR, ndtpConn, s, ErrNDTPCh)
	}
}

func reconnectNDTP(cR redis.Conn, ndtpConn *connection, s *session, ErrNDTPCh chan error) {
	logger := s.logger
	logger.Printf("start reconnecting NDTP")
	for {
		for i := 0; i < 3; i++ {
			if conClosed(ErrNDTPCh) {
				logger.Println("close because of client connection is closed")
				return
			}
			cN, err := net.Dial("tcp", NDTPAddress)
			if err != nil {
				logger.Warningf("can't reconnect to NDTP server: %s", err)
			} else {
				logger.Printf("start sending first message again")
				firstMessage, err := readConnDB(cR, s.id)
				if err != nil {
					logger.Errorf("can't readConnDB: %v", err)
					return
				}
				printPacket(logger,"send first message again: ", firstMessage)
				cN.SetWriteDeadline(time.Now().Add(writeTimeout))
				_, err = cN.Write(firstMessage)
				if err == nil {
					logger.Printf("reconnected", s.id)
					ndtpConn.conn = cN
					ndtpConn.closed = false
					time.Sleep(1 * time.Minute)
					ndtpConn.recon = false
					return
				} else {
					logger.Warningf("failed sending first message again to NDTP server: %s", err)
				}
			}
		}
		time.Sleep(1 * time.Minute)
	}
}
