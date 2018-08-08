package main

import (
	"flag"
	"github.com/gomodule/redigo/redis"
	"log"
	"net"
	"sync"
	"time"
)

const (
	defaultBufferSize = 1024
	headerSize        = 15
	writeTimeout      = 10 * time.Second
)

var (
	listenAddress string
	NDTPAddress   string
	EGTSAddress   string
	egtsConn      connection
	egtsCh        = make(chan rnisData, 200)
	egtsErrCh     chan error
	egtsMu        sync.Mutex
	egtsCr        net.Conn
)

type connection struct {
	addr   string
	conn   net.Conn
	closed bool
	recon  bool
}

type session struct {
	clientNPLReqID uint16
	clientNPHReqID uint32
	serverNPLReqID uint16
	serverNPHReqID uint32
	muC            sync.Mutex
	muS            sync.Mutex
	id             int
}

func main() {
	flag.StringVar(&listenAddress, "l", "", "listen address (e.g. 'localhost:8080')")
	flag.StringVar(&NDTPAddress, "n", "", "send NDTP to address (e.g. 'localhost:8081')")
	flag.StringVar(&EGTSAddress, "e", "", "send EGTS to address (e.g. 'localhost:8082')")
	flag.Parse()
	if listenAddress == "" || NDTPAddress == "" || EGTSAddress == "" {
		flag.Usage()
		return
	}
	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("error while listening: %s", err)
	}
	egtsConn = connection{EGTSAddress, nil, true, false}
	egtsErrCh = make(chan error)
	cE, err := net.Dial("tcp", EGTSAddress)
	if err != nil {
		log.Printf("error while connecting to server: %s", err)
	} else {
		egtsConn.conn = cE
		egtsConn.closed = false
	}
	egtsCr, err := redis.Dial("tcp", ":6379")
	defer egtsCr.Close()
	if err != nil {
		log.Printf("error while connect to redis: %s\n", err)
		return
	}
	go egtsSession()
	go waitReplyEGTS()
	go egtsRemoveExpired()
	connNo := uint64(1)
	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatalf("error while accepting: %s", err)
		}
		log.Printf("accepted connection %d (%s <-> %s)", connNo, c.RemoteAddr(), c.LocalAddr())
		go handleConnection(c, connNo)
		connNo += 1
	}
}

func handleConnection(c net.Conn, connNo uint64) {
	ndtpConn := connection{NDTPAddress, nil, true, false}
	defer c.Close()
	cR, err := redis.Dial("tcp", ":6379")
	defer cR.Close()
	if err != nil {
		log.Printf("error while connect to redis: %s\n", err)
		return
	}
	var b [defaultBufferSize]byte
	n, err := c.Read(b[:])
	if err != nil {
		log.Printf("%d error while getting first message from client %s", connNo, c.RemoteAddr())
		return
	}
	log.Printf("%d got first message from client %s", connNo, c.RemoteAddr())
	firstMessage := b[:n]
	data, dataLen, _, err := parseNDTP(firstMessage)
	if err != nil {
		log.Printf("error: first message is incorrect: %s", err)
		return
	}
	if data.NPH.ServiceID != NPH_SRV_GENERIC_CONTROLS || data.NPH.NPHType != NPH_SGC_CONN_REQUEST {
		log.Printf("error: first message is not conn request. Service: %d, Type %d", data.NPH.ServiceID, data.NPH.NPHType)
	}
	ip := getIP(c)
	log.Printf("conn %d: ip: %s\n", connNo, ip)
	changeAddress(firstMessage, ip)
	err = writeConnDB(cR, data.NPH.ID, firstMessage[:dataLen])
	if err != nil {
		errorReply(c, firstMessage[:dataLen])
		return
	}
	errClientCh := make(chan error)
	ErrNDTPCh := make(chan error)
	cN, err := net.Dial("tcp", NDTPAddress)
	var s session
	s.id = int(data.NPH.ID)
	var mu sync.Mutex
	if err != nil {
		log.Printf("error while connecting to server: %s", err)
	} else {
		ndtpConn.conn = cN
		ndtpConn.closed = false
		send_first_message(cR, &ndtpConn, &s, firstMessage[:dataLen], ErrNDTPCh, &mu)
	}

	connect(cR, c, &ndtpConn, ErrNDTPCh, errClientCh, &s, &mu)
FORLOOP:
	for {
		select {
		case err := <-errClientCh:
			log.Printf("%d error from client: %s", connNo, err)
			break FORLOOP
		}
	}
	close(ErrNDTPCh)
	c.Close()
	ndtpConn.conn.Close()

}

func egtsSession() {
	var buf []byte
	count := 0
	sendTicker := time.NewTicker(100 * time.Millisecond)
	checkTicker := time.NewTicker(1 * time.Second)
	defer sendTicker.Stop()
	defer checkTicker.Stop()
EGTSLOOP:
	for {
		select {
		case <-egtsErrCh:
			break EGTSLOOP
		case message := <-egtsCh:
			packet, egtsMessageID := formEGTS(message)
			count += 1
			buf = append(buf, packet...)
			err := writeEGTSid(egtsMessageID, message.MessageID)
			if err != nil{
				log.Printf("error while write EGTS id",err)
			} else if count == 10 {
				send2egts(buf)
				count = 0
				buf = nil
			}
		case <-sendTicker.C:
			if count < 10 {
				send2egts(buf)
				count = 0
				buf = nil
			}
		case <-checkTicker.C:
			checkOldDataEGTS()
		}
	}
}

func waitReplyEGTS() {
	for {
		var b []byte
		if !egtsConn.closed {
			n, err := egtsConn.conn.Read(b)
			if err != nil {
				log.Printf("error while getting reply from client %s", err)
				go reconnectEGTS()
			}
			if n != 0 {
				egtsMessageID, err := parseEGTS(b)
				if err != nil {
					log.Printf("error while parsing reply from EGTS %s", err)
				} else {
					err := deleteEGTSid(egtsMessageID)
					if err != nil {
						log.Printf("error while delete EGTS id %s", err)
					}
				}
			}
		}
	}
}

func send2egts(buf []byte) {
	if !egtsConn.closed {
		egtsConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		_, err := egtsConn.conn.Write(buf)
		if err != nil {
			go reconnectEGTS()
		}
	}
}

func send_first_message(cR redis.Conn, ndtpConn *connection, s *session, firstMessage []byte, ErrNDTPCh chan error, mu *sync.Mutex) {
	ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := ndtpConn.conn.Write(firstMessage)
	if err != nil {
		ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
	}
}

func connect(cR redis.Conn, origin net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, s *session, mu *sync.Mutex) {
	go clientSession(cR, origin, ndtpConn, ErrNDTPCh, errClientCh, s, mu)
	go serverSession(cR, origin, ndtpConn, ErrNDTPCh, errClientCh, s, mu)
}

func serverSession(cR redis.Conn, client net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, s *session, mu *sync.Mutex) {
	for {
		if conClosed(ErrNDTPCh) {
			return
		}
		if !ndtpConn.closed {
			var b []byte
			_, err := ndtpConn.conn.Read(b)
			if err != nil {
				ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
				continue
			}
			var restBuf []byte
			for {
				var data ndtpData
				var packetLen uint16
				data, packetLen, restBuf, err = parseNDTP(restBuf)
				if err != nil {
					log.Println(err)
					break
				}
				if !data.valid {
					restBuf = restBuf[packetLen:]
					continue
				}
				if data.NPH.isResult {
					removeFromNDTP(cR, s.id, data.NPH.NPHReqID)
				} else {
					client.SetWriteDeadline(time.Now().Add(writeTimeout))
					message := changePacketFromServ(b[:packetLen], s)
					_, err = client.Write(message)
					if err != nil {
						errClientCh <- err
						return
					}
				}
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func clientSession(cR redis.Conn, client net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, s *session, mu *sync.Mutex) {
	var restBuf []byte
	checkTicker := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-checkTicker.C:
			checkOldDataNDTP(cR, s, ndtpConn, mu, s.id, ErrNDTPCh)
		default:
			var b []byte
			n, err := client.Read(b)
			if err != nil {
				errClientCh <- err
				return
			}
			restBuf = append(restBuf, b[:n]...)
			for {
				var data ndtpData
				var packetLen uint16
				data, packetLen, restBuf, err = parseNDTP(restBuf)
				if err != nil {
					if len(restBuf) > defaultBufferSize {
						restBuf = []byte{}
					}
					log.Println(err)
					break
				}
				mill := getMill()
				err = write2DB(cR, data, s, restBuf[:packetLen], mill)
				if err != nil {
					errorReply(client, restBuf[:packetLen])
					restBuf = []byte{}
					break
				}
				if ndtpConn.closed != true {
					NPHReqID, message := changePacket(restBuf[:packetLen], data, s)
					err = writeNDTPid(cR, data.NPH.ID, NPHReqID, mill)
					if err != nil {
						log.Println(err)
					} else {
						ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
						_, err = ndtpConn.conn.Write(message)
						if err != nil {
							ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
						}
					}
				}
				if egtsConn.closed != true {
					if toEGTS(data) {
						egtsCh <- data.ToRnis
					}
				}
				err = reply(client, data.NPH, restBuf[:packetLen])
				if err != nil {
					errClientCh <- err
					return
				}
				restBuf = restBuf[packetLen:]
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
}

func toEGTS(data ndtpData) bool {
	if data.ToRnis.Time != 0 {
		return true
	}
	return false
}

func ndtpConStatus(cR redis.Conn, ndtpConn *connection, s *session, mu *sync.Mutex, ErrNDTPCh chan error) {
	mu.Lock()
	if ndtpConn.closed || ndtpConn.recon {
		return
	} else {
		ndtpConn.recon = true
		ndtpConn.conn.Close()
		ndtpConn.closed = true
		go reconnectNDTP(cR, ndtpConn, s, ErrNDTPCh)
	}
	mu.Unlock()
}

func reconnectNDTP(cR redis.Conn, ndtpConn *connection, s *session, ErrNDTPCh chan error) {
	for {
		if conClosed(ErrNDTPCh) {
			return
		}
		cN, err := net.Dial("tcp", NDTPAddress)
		if err != nil {
			log.Printf("error while connecting to server: %s", err)
		} else {
			firstMessage, err := readConnDB(cR, s.id)
			if err != nil {
				log.Println("reconnecting error")
				return
			}
			ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			_, err = ndtpConn.conn.Write(firstMessage)
			if err == nil {
				ndtpConn.conn = cN
				ndtpConn.closed = false
				time.Sleep(1 * time.Minute)
				ndtpConn.recon = false
			}
		}

	}
	return
}

func reconnectEGTS() {
	egtsMu.Lock()
	if egtsConn.closed || egtsConn.recon {
		return
	} else {
		egtsConn.recon = true
		egtsConn.conn.Close()
		egtsConn.closed = true
	}
	egtsMu.Unlock()
	for {
		for i := 0; i < 3; i++ {
			cE, err := net.Dial("tcp", EGTSAddress)
			if err == nil {
				egtsConn.conn = cE
				egtsConn.closed = false
				return
			}
			log.Printf("error while connecting to server: %s", err)
		}
		time.Sleep(10 * time.Second)
	}
}

func conClosed(ErrNDTPCh chan error) bool {
	select {
	case <-ErrNDTPCh:
		return true
	default:
		return false
	}
}

func reply(c net.Conn, data nphData, packet []byte) error {
	if data.isResult {
		return nil
	} else {
		ans := answer(packet)
		c.SetWriteDeadline(time.Now().Add(writeTimeout))
		_, err := c.Write(ans)
		return err
	}
}
func errorReply(c net.Conn, packet []byte) error {
	ans := errorAnswer(packet)
	c.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := c.Write(ans)
	return err

}

func serverID(s *session) (uint16, uint32) {
	s.muS.Lock()
	nplID := s.serverNPLReqID
	nphID := s.serverNPHReqID
	s.serverNPLReqID++
	s.serverNPHReqID++
	s.muS.Unlock()
	return nplID, nphID
}
func clientID(s *session) (uint16, uint32) {
	s.muC.Lock()
	nplID := s.clientNPLReqID
	nphID := s.clientNPHReqID
	s.clientNPLReqID++
	s.clientNPHReqID++
	s.muC.Unlock()
	return nplID, nphID
}

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
			mill := getMill()
			NPHReqID, message := changePacket(mes, data, s)
			err = writeNDTPid(cR, data.NPH.ID, NPHReqID, mill)
			if err != nil {
				log.Println(err)
			} else {
				ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				_, err = ndtpConn.conn.Write(message)
				if err != nil {
					ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
				}
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func egtsRemoveExpired() {
	for {
		err := removeExpiredDataEGTS()
		if err != nil {
			log.Println("error while remove expired data EGTS %s",err)
		}
		time.Sleep(1 * time.Hour)
	}
}

func checkOldDataEGTS() (err error) {
	messages, err := getOldEGTS()
	if err != nil {
		log.Println("can't get old EGTS %s",err)
		return
	}
	
	var bufOld []byte
	for _, msg := range messages {
		var dataNDTP ndtpData
		dataNDTP, _, _, err = parseNDTP(msg)
		if err != nil {
			packet, egtsMessageID := formEGTS(dataNDTP.ToRnis)
			bufOld = append(bufOld, packet...)
			err := writeEGTSid(egtsMessageID, dataNDTP.ToRnis.MessageID)
			if err != nil{
				log.Printf("error while write EGTS id",err)
			}
		}
	}
	send2egts(bufOld)
	return
}
