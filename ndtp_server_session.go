package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"log"
	"net"
	"sync"
	"time"
)

//session with NDTP server
func serverSession(client net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, s *session, mu *sync.Mutex) {
	//connect to redis
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			log.Printf("serverSession: error connecting to redis in serverSession: %s\n", err)
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
			checkOldDataNDTPServ(cR, s, client, s.id)
		default:
			//check if connection with client is closed
			if conClosed(ErrNDTPCh) {
				return
			}
			//if connection to client is not closed, reading data from server
			if !ndtpConn.closed {
				var b [defaultBufferSize]byte
				//todo set reading timeout
				n, err := ndtpConn.conn.Read(b[:])
				log.Printf("serverSession: ndtpConn.closed = %t; ndtpConn.recon = %t", ndtpConn.closed, ndtpConn.recon)
				log.Printf("serverSession: received %d bytes from server", n)
				printPacket("serverSession: packet from server: ", b[:n])
				if err != nil {
					log.Printf("serverSession: error while getting data from server: %v", err)
					ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
					time.Sleep(5 * time.Second)
					continue
				}
				restBuf = append(restBuf, b[:n]...)
				log.Printf("serverSession: len(restBuf) = %d", len(restBuf))
				for {
					var data ndtpData
					var packet []byte
					data, packet, restBuf, err = parseNDTP(restBuf)
					if err != nil {
						log.Printf("serverSession: error while parsing NDTP from server: %v", err)
						restBuf = []byte{}
						break
					}
					if !data.valid {
						restBuf = nil
						continue
					}
					if data.NPH.isResult {
						err = removeFromNDTP(cR, s.id, data.NPH.NPHReqID)
					} else if data.NPH.ServiceID == NPH_SRV_EXTERNAL_DEVICE {
						err = handleExtDevMes(cR, client, ndtpConn, errClientCh, s, data, packet)
					} else {
						err = handlePacket(cR, client, errClientCh, s, data, packet)
					}
					if err != nil {
						log.Printf("serverSession: error: %s", err)
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

func handlePacket(cR redis.Conn, client net.Conn, errClientCh chan error, s *session, data ndtpData, packet []byte) (err error) {
	client.SetWriteDeadline(time.Now().Add(writeTimeout))
	printPacket("handlePacket: before changing control message: ", packet)
	mill := getMill()
	packetCopy := make([]byte, len(packet))
	copy(packetCopy, packet)
	err = write2DBServer(cR, s, packetCopy, mill)
	if err != nil {
		log.Printf("handlePacket: error write2DBServer: %v", err)
		return
	}
	NPHReqID, message := changePacketFromServ(packet, s)
	err = writeNDTPidServ(cR, s.id, uint32(NPHReqID), mill)
	printPacket("handlePacket: send control message to client: ", message)
	_, err = client.Write(message)
	if err != nil {
		errClientCh <- err
		return
	}
	return
}

func handleExtDevMes(cR redis.Conn, client net.Conn, ndtpConn *connection, errClientCh chan error, s *session, data ndtpData, packet []byte) (err error) {
	if data.NPH.NPHType == NPH_SED_DEVICE_TITLE_DATA || data.NPH.NPHType == NPH_SED_DEVICE_DATA {
		log.Printf("handleExtDevMes: handle NPH_SRV_EXTERNAL_DEVICE type: %d, id: %d, packetNum: %d", data.NPH.NPHType, data.ext.mesID, data.ext.packNum)
		data.NPH.ID = uint32(s.id)
		packetCopy := make([]byte, len(packet))
		copy(packetCopy, packet)
		mill := getMill()
		err = write2DBServer(cR, s, packetCopy, mill)
		if err != nil {
			log.Println("handleExtDevMes: send ext error reply to server because of: ", err)
			errorReplyExt(ndtpConn.conn, data.ext.mesID, data.ext.packNum, packetCopy)
			return
		}
		log.Println("handleExtDevMes: start to send ext device message to NDTP server")
		packetCopyNDTP := make([]byte, len(packet))
		copy(packetCopyNDTP, packet)
		_, message := changePacketFromServ(packetCopyNDTP, s)
		printPacket("handleExtDevMes: packet after changing ext device message: ", message)
		err = writeNDTPIdExtServ(cR, s.id, data.ext.mesID, data.ext.packNum, mill)
		if err != nil {
			log.Printf("handleExtDevMes: error writeNDTPIdExt %v", err)
			errorReplyExt(ndtpConn.conn, data.ext.mesID, data.ext.packNum, packetCopy)
			return
		} else {
			client.SetWriteDeadline(time.Now().Add(writeTimeout))
			printPacket("handleExtDevMes: send ext device message to server: ", message)
			_, err = client.Write(message)
			if err != nil {
				log.Printf("handleExtDevMes: send ext device message to NDTP server error: %s", err)
				errClientCh <- err
				return
			}
		}
		log.Println("handleExtDevMes: start to reply to ext device message")
		err = replyExt(ndtpConn.conn, data.ext.mesID, data.ext.packNum, packet)
		if err != nil {
			log.Println("handleExtDevMes: error replying to ext device message: ", err)
			errClientCh <- err
			return
		}
	} else {
		if data.NPH.NPHType == NPH_SED_DEVICE_RESULT {
			err = removeFromNDTPExt(cR, s.id, data.ext.mesID, data.ext.packNum)
			if err != nil {
				log.Printf("handleExtDevMes: removeFromNDTPExt error for id %d : %v", s.id, err)
			}
		} else {
			err = fmt.Errorf("handle NPH_SRV_EXTERNAL_DEVICE type: %d, id: %d, packetNum: %d", data.NPH.NPHType, data.ext.mesID, data.ext.packNum)
		}
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
