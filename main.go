package main

import (
	"flag"
	"github.com/ashirko/go-metrics"
	"github.com/ashirko/go-metrics-graphite"
	"github.com/gomodule/redigo/redis"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultBufferSize = 1024
	headerSize        = 15
	writeTimeout      = 10 * time.Second
	readTimeout       = 10 * time.Second
)

var (
	listenAddress   string
	NDTPAddress     string
	EGTSAddress     string
	graphiteAddress string
	egtsConn        connection
	egtsCh          = make(chan rnisData, 10000)
	egtsMu          sync.Mutex
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

var (
	countClientNDTP     metrics.Counter
	countToServerNDTP   metrics.Counter
	countFromServerNDTP metrics.Counter
	countServerEGTS     metrics.Counter
	enableMetrics       bool
)

func main() {
	flag.StringVar(&listenAddress, "l", "", "listen address (e.g. 'localhost:8080')")
	flag.StringVar(&NDTPAddress, "n", "", "send NDTP to address (e.g. 'localhost:8081')")
	flag.StringVar(&EGTSAddress, "e", "", "send EGTS to address (e.g. 'localhost:8082')")
	flag.StringVar(&graphiteAddress, "g", "", "graphite address (e.g. 'localhost:8083')")
	flag.Parse()
	if listenAddress == "" || NDTPAddress == "" || EGTSAddress == "" {
		flag.Usage()
		return
	}
	if graphiteAddress == "" {
		log.Println("don't send metrics to graphite")
	} else {
		addr, err := net.ResolveTCPAddr("tcp", graphiteAddress)
		if err != nil {
			log.Printf("error while connection to graphite: %s\n", err)
		} else {
			countClientNDTP = metrics.NewCustomCounter()
			countToServerNDTP = metrics.NewCustomCounter()
			countFromServerNDTP = metrics.NewCustomCounter()
			countServerEGTS = metrics.NewCustomCounter()
			metrics.Register("clNDTP", countClientNDTP)
			metrics.Register("toServNDTP", countToServerNDTP)
			metrics.Register("fromServNDTP", countFromServerNDTP)
			metrics.Register("servEGTS", countServerEGTS)
			enableMetrics = true
			log.Println("start sending metrics to graphite")
			go graphite.Graphite(metrics.DefaultRegistry, 5*10e8, "ndtpserv.metrics", addr)
		}
	}
	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("error while listening: %s", err)
	}
	egtsConn = connection{EGTSAddress, nil, true, false}
	cE, err := net.Dial("tcp", EGTSAddress)
	if err != nil {
		log.Printf("error while connecting to EGTS server: %s", err)
	} else {
		egtsConn.conn = cE
		egtsConn.closed = false
	}
	egtsCr, err := redis.Dial("tcp", ":6379")
	defer egtsCr.Close()
	if err != nil {
		log.Printf("error while connect to redis 1: %s\n", err)
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
		log.Printf("handleConnection: error connecting to redis in handleConnection: %s\n", err)
		return
	}
	var b [defaultBufferSize]byte
	n, err := c.Read(b[:])
	if err != nil {
		log.Printf("handleConnection: %d error while getting first message from client %s", connNo, c.RemoteAddr())
		return
	}
	if enableMetrics {
		countClientNDTP.Inc(1)
	}
	log.Printf("handleConnection: %d got first message from client %s", connNo, c.RemoteAddr())
	firstMessage := b[:n]
	data, packet, _, err := parseNDTP(firstMessage)
	if err != nil {
		log.Printf("handleConnection: error: first message is incorrect: %s", err)
		return
	}
	if data.NPH.ServiceID != NPH_SRV_GENERIC_CONTROLS || data.NPH.NPHType != NPH_SGC_CONN_REQUEST {
		log.Printf("handleConnection: error first message is not conn request. Service: %d, Type %d", data.NPH.ServiceID, data.NPH.NPHType)
	}
	ip := getIP(c)
	log.Printf("handleConnection: conn %d: ip: %s\n", connNo, ip)
	printPacket("handleConnection: before change first packet: ", packet)
	changeAddress(packet, ip)
	printPacket("handleConnection: after change first packet: ", packet)
	err = writeConnDB(cR, data.NPH.ID, packet)
	replyCopy := make([]byte, len(packet))
	copy(replyCopy, packet)
	if err != nil {
		errorReply(c, replyCopy)
		return
	} else {
		reply(c, data.NPH, replyCopy)
	}
	errClientCh := make(chan error)
	ErrNDTPCh := make(chan error)
	cN, err := net.Dial("tcp", NDTPAddress)
	var s session
	s.id = int(data.NPH.ID)
	var mu sync.Mutex
	if err != nil {
		log.Printf("handleConnection: error while connecting to NDTP server: %s", err)
	} else {
		ndtpConn.conn = cN
		ndtpConn.closed = false
		sendFirstMessage(cR, &ndtpConn, &s, packet, ErrNDTPCh, &mu)
		cR.Close()
	}
	connect(c, &ndtpConn, ErrNDTPCh, errClientCh, &s, &mu)
FORLOOP:
	for {
		select {
		case err := <-errClientCh:
			log.Printf("handleConnection: %d error from client: %s", connNo, err)
			break FORLOOP
		}
	}
	close(ErrNDTPCh)
	c.Close()
	ndtpConn.conn.Close()

}

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

func sendFirstMessage(cR redis.Conn, ndtpConn *connection, s *session, firstMessage []byte, ErrNDTPCh chan error, mu *sync.Mutex) {
	ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	printPacket("sending first packet: ", firstMessage)
	_, err := ndtpConn.conn.Write(firstMessage)
	if err != nil {
		ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
	}
}

func connect(origin net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, s *session, mu *sync.Mutex) {
	go clientSession(origin, ndtpConn, ErrNDTPCh, errClientCh, s, mu)
	go serverSession(origin, ndtpConn, ErrNDTPCh, errClientCh, s, mu)
}

func serverSession(client net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, s *session, mu *sync.Mutex) {
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
	defer cR.Close()
	for {
		if conClosed(ErrNDTPCh) {
			return
		}
		if !ndtpConn.closed {
			var b [defaultBufferSize]byte
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
			var restBuf []byte
			restBuf = b[:n]
			for {
				var data ndtpData
				var packet []byte
				data, packet, restBuf, err = parseNDTP(restBuf)
				if err != nil {
					log.Printf("serverSession: error while parsing NDTP from server: %v", err)
					break
				}
				if !data.valid {
					restBuf = nil
					continue
				}
				if data.NPH.isResult {
					log.Printf("serverSession: got NPH Result for id %d, reqID: %d", s.id, data.NPH.NPHReqID)
					err = removeFromNDTP(cR, s.id, data.NPH.NPHReqID)
					if err != nil {
						log.Printf("serverSession: removeFromNDTP error for id %d, reqID %d: %v", s.id, data.NPH.NPHReqID, err)
					}
				} else if data.NPH.ServiceID == NPH_SRV_EXTERNAL_DEVICE {
					log.Printf("serverSession: handle NPH_SRV_EXTERNAL_DEVICE type: %d, id: %d, packetNum: %d", data.NPH.NPHType, data.ext.mesID, data.ext.packNum)
					if data.NPH.NPHType != NPH_SED_DEVICE_RESULT {
						log.Println("serverSession: error received external message but not result")
					} else {
						err = removeFromNDTPExt(cR, s.id, data.ext.mesID, data.ext.packNum)
						if err != nil {
							log.Printf("serverSession: removeFromNDTPExt error for id %d : %v", s.id, err)
						}
					}

				} else {
					client.SetWriteDeadline(time.Now().Add(writeTimeout))
					printPacket("serverSession: before changing control message: ", packet)
					reqID, message := changePacketFromServ(packet, s)
					writeControlID(cR, s.id, reqID, data.NPH.NPHReqID)
					printPacket("serverSession: send control message to client: ", message)
					_, err = client.Write(message)
					if err != nil {
						errClientCh <- err
						return
					}
				}
				if len(restBuf) == 0 {
					break
				}
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func clientSession(client net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, s *session, mu *sync.Mutex) {
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
					controlReplyID, err := readControlID(cR, s.id, int(data.NPH.NPHReqID))
					if err == nil {
						printPacket("clientSession: control message before changing: ", packet)
						message := changeContolResult(packet, controlReplyID)
						printPacket("clientSession: control message before changing: ", message)
						ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
						printPacket("clientSession: send control message to server: ", message)
						_, err = ndtpConn.conn.Write(message)
						if err != nil {
							log.Printf("clientSession: send to NDTP server error: %s", err)
							ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
						}
					}
				} else if !data.NPH.needReply {
					log.Printf("clientSession: not need reply on message servId: %d, type: %d", data.NPH.ServiceID, data.NPH.NPHType)
					packetCopyNDTP := make([]byte, len(packet))
					copy(packetCopyNDTP, packet)
					_, message := changePacket(packetCopyNDTP, data, s)
					printPacket("clientSession: packet after changing (no reply): ", message)
					ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
					printPacket("clientSession: send message to server (no reply): ", message)
					_, err = ndtpConn.conn.Write(message)
					if err != nil {
						log.Printf("clientSession: send to NDTP server error (no reply): %s", err)
						ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
					}
				} else if data.NPH.ServiceID == NPH_SRV_EXTERNAL_DEVICE {
					log.Printf("clientSession: handle NPH_SRV_EXTERNAL_DEVICE type: %d, id: %d, packetNum: %d", data.NPH.NPHType, data.ext.mesID, data.ext.packNum)
					data.NPH.ID = uint32(s.id)
					packetCopy := make([]byte, len(packet))
					copy(packetCopy, packet)
					if data.NPH.NPHType == NPH_SED_DEVICE_TITLE_DATA || data.NPH.NPHType == NPH_SED_DEVICE_DATA {
						err = write2DB(cR, data, s, packetCopy, mill)
						if err != nil {
							log.Println("clientSession: send ext error reply to server because of: ", err)
							errorReplyExt(client, data.ext.mesID, data.ext.packNum, packetCopy)
							restBuf = []byte{}
							break
						}
						log.Println("clientSession: start to send ext device message to NDTP server")
					}
					if ndtpConn.closed != true {
						packetCopyNDTP := make([]byte, len(packet))
						copy(packetCopyNDTP, packet)
						_, message := changePacket(packetCopyNDTP, data, s)
						printPacket("clientSession: packet after changing ext device message: ", message)
						err = writeNDTPIdExt(cR, s.id, data.ext.mesID, data.ext.packNum, mill)
						if err != nil {
							log.Printf("error writeNDTPIdExt: %v", err)
						} else {
							ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
							printPacket("clientSession: send ext device message to server: ", message)
							_, err = ndtpConn.conn.Write(message)
							if err != nil {
								log.Printf("clientSession: send ext device message to NDTP server error: %s", err)
								ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
							} else {
								if enableMetrics {
									countToServerNDTP.Inc(1)
								}
							}
						}
					}
					log.Println("clientSession: start to reply to ext device message")
					err = replyExt(client, data.ext.mesID, data.ext.packNum, packet)
					if err != nil {
						log.Println("clientSession: error replying to ext device message: ", err)
						errClientCh <- err
						return
					}
					time.Sleep(1 * time.Millisecond)
					if len(restBuf) == 0 {
						break
					}

				} else {
					data.NPH.ID = uint32(s.id)
					packetCopy := make([]byte, len(packet))
					copy(packetCopy, packet)
					err = write2DB(cR, data, s, packetCopy, mill)
					if err != nil {
						log.Println("clientSession: send error reply to server because of: ", err)
						errorReply(client, packetCopy)
						restBuf = []byte{}
						break
					}
					log.Println("clientSession: start to send to NDTP server")
					if ndtpConn.closed != true {
						packetCopyNDTP := make([]byte, len(packet))
						copy(packetCopyNDTP, packet)
						NPHReqID, message := changePacket(packetCopyNDTP, data, s)
						printPacket("clientSession: packet after changing: ", message)
						err = writeNDTPid(cR, s.id, NPHReqID, mill)
						if err != nil {
							log.Printf("error writingNDTPid: %v", err)
						} else {
							ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
							printPacket("clientSession: send message to server: ", message)
							_, err = ndtpConn.conn.Write(message)
							if err != nil {
								log.Printf("clientSession: send to NDTP server error: %s", err)
								ndtpConStatus(cR, ndtpConn, s, mu, ErrNDTPCh)
							} else {
								if enableMetrics {
									countToServerNDTP.Inc(1)
								}
							}
						}
					}
					data.ToRnis.messageID = strconv.Itoa(s.id) + ":" + strconv.FormatInt(mill, 10)
					//log.Println("EGTS closed: ", egtsConn.closed)
					if egtsConn.closed != true {
						if toEGTS(data) {
							log.Println("clientSession: start to send to EGTS server")
							data.ToRnis.id = uint32(s.id)
							egtsCh <- data.ToRnis
						}
					}
					log.Println("clientSession: start to reply")
					err = reply(client, data.NPH, packet)
					if err != nil {
						log.Println("clientSession: error replying to att: ", err)
						errClientCh <- err
						return
					}
					//restBuf = restBuf[packetLen:]
					time.Sleep(1 * time.Millisecond)
					if len(restBuf) == 0 {
						break
					}

				}

			}
		}
	}
}

func toEGTS(data ndtpData) bool {
	if data.ToRnis.time != 0 {
		return true
	}
	return false
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
	log.Printf("reconnectNDTP: start reconnect NDTP for id %d", s.id)
	for {
		for i := 0; i < 3; i++ {
			if conClosed(ErrNDTPCh) {
				return
			}
			cN, err := net.Dial("tcp", NDTPAddress)
			if err != nil {
				log.Printf("reconnectNDTP: error while reconnecting to NDPT server: %s", err)
			} else {
				log.Printf("reconnectNDTP: send first message again to %d", s.id)
				firstMessage, err := readConnDB(cR, s.id)
				if err != nil {
					log.Printf("error readConnDB: %v", err)
					return
				}
				cN.SetWriteDeadline(time.Now().Add(writeTimeout))
				printPacket("reconnectNDTP: send first message again: ", firstMessage)
				_, err = cN.Write(firstMessage)
				if err == nil {
					log.Printf("reconnectNDTP: id %d reconnected", s.id)
					ndtpConn.conn = cN
					ndtpConn.closed = false
					time.Sleep(1 * time.Minute)
					ndtpConn.recon = false
					return
				} else {
					log.Printf("reconnectNDTP: error while send first message again to NDTP server: %s", err)
				}
			}
		}
		time.Sleep(1 * time.Minute)
	}
}

func egtsConStatus() {
	log.Println("start egtsConStatus")
	egtsMu.Lock()
	defer egtsMu.Unlock()
	log.Printf("egtsConStatus closed: %t; recon: %t", egtsConn.closed, egtsConn.recon)
	if egtsConn.closed || egtsConn.recon {
		return
	} else {
		egtsConn.recon = true
		egtsConn.conn.Close()
		egtsConn.closed = true
		go reconnectEGTS()
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
		printPacket("reply: send answer: ", ans)
		_, err := c.Write(ans)
		return err
	}
}
func errorReply(c net.Conn, packet []byte) error {
	ans := errorAnswer(packet)
	c.SetWriteDeadline(time.Now().Add(writeTimeout))
	printPacket("errorReply: send error reply: ", ans)
	_, err := c.Write(ans)
	return err

}
func replyExt(c net.Conn, mesID, packNum uint16, packet []byte) error {
	ans := answerExt(packet, mesID, packNum)
	c.SetWriteDeadline(time.Now().Add(writeTimeout))
	printPacket("replyExt: send answer: ", ans)
	_, err := c.Write(ans)
	return err
}
func errorReplyExt(c net.Conn, mesID, packNum uint16, packet []byte) error {
	ans := errorAnswerExt(packet, mesID, packNum)
	c.SetWriteDeadline(time.Now().Add(writeTimeout))
	printPacket("errorReplyExt: send error reply: ", ans)
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
	messages, err := getOldEGTS(cR)
	if err != nil {
		log.Printf("can't get old EGTS %s", err)
		return
	}
	var bufOld []byte
	var i int
	for _, msg := range messages {
		if i < 10 {
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

func printPacket(s string, slice []byte) {
	sliceText := []string{}
	for i := range slice {
		number := slice[i]
		text := strconv.Itoa(int(number))
		sliceText = append(sliceText, text)
	}
	result := strings.Join(sliceText, ",")
	log.Printf("%s {%s}\n", s, result)
}
