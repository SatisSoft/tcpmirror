package main

import (
	"fmt"
	"net"
	"time"
)

import nav "github.com/ashirko/navprot"

//session with NDTP server
func serverSession(s *session) {
	var restBuf []byte
	go oldFromServer(s)
	//receive, process and send data to a client
	for {
		//check if connection with client is closed
		if conClosed(s) {
			return
		}
		//if connection to client is not closed, reading data from server
		if !s.servConn.closed {
			restBuf = waitServerMessage(s, restBuf)
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func waitServerMessage(s *session, restBuf []byte) []byte {
	var b [defaultBufferSize]byte
	err := s.servConn.conn.SetReadDeadline(time.Now().Add(readTimeout))
	if err != nil {
		s.logger.Warningf("can't set read dead line: %s", err)
	}
	n, err := s.servConn.conn.Read(b[:])
	s.logger.Debugf("servConn.closed = %t; servConn.recon = %t", s.servConn.closed, s.servConn.recon)
	s.logger.Debugf("received %d bytes from server", n)
	printPacket(s.logger, "packet from server: ", b[:n])
	if err != nil {
		s.logger.Warningf("can't get data from server: %v", err)
		ndtpConStatus(s)
		time.Sleep(5 * time.Second)
		return restBuf
	}
	restBuf = append(restBuf, b[:n]...)
	s.logger.Debugf("len(restBuf) = %d", len(restBuf))
	for {
		ndtp := new(nav.NDTP)
		restBuf, err = ndtp.Parse(restBuf)
		if err != nil {
			s.logger.Warningf(" error while parsing NDTP from server: %v", err)
			restBuf = []byte{}
			break
		}

		if ndtp.IsResult() {
			err = nphResultFromServer(s, ndtp)
		} else if ndtp.Service() == nav.NphSrvExternalDevice {
			err = extFromServer(s, ndtp)
		} else {
			err = controlFromServer(s, ndtp)
		}
		if err != nil {
			s.logger.Warningf("can't process message from server: %s", err)
			restBuf = []byte{}
			break
		}
		time.Sleep(1 * time.Millisecond)
		if len(restBuf) == 0 {
			break
		}
	}
	return restBuf
}

func nphResultFromServer(s *session, ndtp *nav.NDTP) (err error) {
	res := ndtp.Nph.Data.(uint32)
	if res == 0 {
		err = removeFromNDTP(s, ndtp.Nph.ReqID)
	} else {
		s.logger.Warningf("nph result error: %d", res)
	}
	return
}

func controlFromServer(s *session, ndtp *nav.NDTP) (err error) {
	printPacket(s.logger, "before changing control message: ", ndtp.Packet)
	nphReqID := int(s.clientNphID())
	changes := map[string]int{nav.NplReqID: int(s.clientNplID()), nav.NphReqID: nphReqID}
	ndtp.ChangePacket(changes)
	s.logger.Debugf("old nphReqID: %d; new nphReqID: %d", ndtp.Nph.ReqID, nphReqID)
	if err = writeControlID(s, nphReqID, ndtp.Nph.ReqID); err != nil {
		s.logger.Errorf("can't writeControlID: %s", err)
	}
	printPacket(s.logger, "send control message to client: ", ndtp.Packet)
	err = sendToClient(s, ndtp.Packet)
	if err != nil {
		s.errClientCh <- err
		return
	}
	return
}

func extFromServer(s *session, ndtp *nav.NDTP) (err error) {
	s.logger.Debugf("handle NPH_SRV_EXTERNAL_DEVICE")
	pType := ndtp.PacketType()
	if pType == nav.NphSedDeviceTitleData {
		err = extTitleFromServer(s, ndtp)
	} else if pType == nav.NphSedDeviceResult {
		err = extResFromServer(s, ndtp)
	} else {
		err = fmt.Errorf("unknown NPHType: packet %v", ndtp.Packet)
	}
	return
}

func extTitleFromServer(s *session, ndtp *nav.NDTP) (err error) {
	packetCopy := append([]byte(nil), ndtp.Packet...)
	mill := getMill()
	err = writeExtServ(s, packetCopy, mill, ndtp.Nph.Data.(nav.ExtDevice).MesID)
	if err != nil {
		s.logger.Errorf("can't writeExtServ: %s", err)
		return
	}
	s.logger.Debugf("start to send ext device message to NDTP server")
	changes := map[string]int{nav.NplReqID: int(s.clientNplID()), nav.NphReqID: int(s.clientNphID())}
	ndtp.ChangePacket(changes)
	printPacket(s.logger, "send ext device message to client: ", ndtp.Packet)
	err = sendToClient(s, ndtp.Packet)
	if err != nil {
		s.logger.Warningf("can't send ext device message to NDTP server: %s", err)
		s.errClientCh <- err
	}
	return
}

func extResFromServer(s *session, ndtp *nav.NDTP) (err error) {
	if ndtp.Nph.Data.(nav.ExtDevice).Res == 0 {
		s.logger.Debugln("received result and remove data from db")
		err = removeFromNDTPExt(s, ndtp.Nph.Data.(nav.ExtDevice).MesID)
		if err != nil {
			s.logger.Errorf("can't removeFromNDTPExt: %v", err)
		}
	} else {
		err = fmt.Errorf("received ext result message with error status for id %d", s.id)
	}
	return
}

func conClosed(s *session) bool {
	select {
	case <-s.errClientCh:
		return true
	default:
		return false
	}
}

func ndtpConStatus(s *session) {
	s.muRecon.Lock()
	defer s.muRecon.Unlock()
	if s.servConn.closed || s.servConn.recon {
		return
	}
	s.servConn.recon = true
	if err := s.servConn.conn.Close(); err != nil {
		s.logger.Errorf("can't close servConn: %s", err)
	}
	s.servConn.closed = true
	go reconnectNDTP(s)
}

func reconnectNDTP(s *session) {
	s.logger.Printf("start reconnecting NDTP")
	for {
		for i := 0; i < 3; i++ {
			if conClosed(s) {
				s.logger.Println("close because of client connection is closed")
				return
			}
			cN, err := net.Dial("tcp", ndtpServer)
			if err != nil {
				s.logger.Warningf("can't reconnect to NDTP server: %s", err)
			} else {
				s.logger.Printf("start sending first message again")
				firstMessage, err := readConnDB(s)
				if err != nil {
					s.logger.Errorf("can't readConnDB: %v", err)
					return
				}
				printPacket(s.logger, "send first message again: ", firstMessage)
				err = cN.SetWriteDeadline(time.Now().Add(writeTimeout))
				if err != nil {
					s.logger.Errorf("can't setWriteDeadline")
				}
				_, err = cN.Write(firstMessage)
				if err == nil {
					s.logger.Printf("reconnected")
					s.servConn.conn = cN
					s.servConn.closed = false
					time.Sleep(1 * time.Minute)
					s.servConn.recon = false
					return
				}
				s.logger.Warningf("failed sending first message again to NDTP server: %s", err)
			}
		}
		time.Sleep(1 * time.Minute)
	}
}
