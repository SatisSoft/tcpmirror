package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"log"
	"net"
	"strings"
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
)

type connection struct {
	addr   string
	conn   net.Conn
	closed bool
	recon  bool
}

func main() {
	flag.StringVar(&listenAddress, "l", "", "listen address (e.g. 'localhost:8080')")
	flag.StringVar(&NDTPAddress, "m", "", "send NDTP to address (e.g. 'localhost:8081')")
	flag.StringVar(&EGTSAddress, "r", "", "send EGTS to address (e.g. 'localhost:8081')")
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
	go egtsSession()
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
		//TODO write errorRequest function
		errorRequest(c)
		return
	}

	errClientCh := make(chan error)
	ErrNDTPCh := make(chan error)
	cN, err := net.Dial("tcp", NDTPAddress)
	var mu sync.Mutex
	if err != nil {
		log.Printf("error while connecting to server: %s", err)
	} else {
		ndtpConn.conn = cN
		ndtpConn.closed = false
		send_first_message(&ndtpConn, firstMessage[:dataLen], ErrNDTPCh, &mu)
	}

	connect(c, &ndtpConn, ErrNDTPCh, errClientCh, connNo, &mu)

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
EGTSLOOP:
	for {
		select {
		case _ := <-egtsErrCh:
			break EGTSLOOP
		case message := <-egtsCh:
			packet := formEGTS(message)
			egtsConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			_, err := egtsConn.conn.Write(packet)
			if err != nil {
				reconnectEGTS()
			}
		}
	}
}

func send_first_message(ndtpConn *connection, firstMessage []byte, ErrNDTPCh chan error, mu *sync.Mutex) {
	ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := ndtpConn.conn.Write(firstMessage)
	if err != nil {
		ndtpConStatus(ndtpConn, mu, ErrNDTPCh)
	}
}

func connect(origin net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, connNo uint64, mu *sync.Mutex) {
	go client2servers(origin, ndtpConn, ErrNDTPCh, errClientCh, connNo, mu)
	go server2client(origin, ndtpConn, ErrNDTPCh, errClientCh, mu)
}

func server2client(to net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, mu *sync.Mutex) {
	for {
		if !ndtpConn.closed {
			var b []byte
			_, err := ndtpConn.conn.Read(b)
			if err != nil {
				ndtpConStatus(ndtpConn, mu, ErrNDTPCh)
				continue
			}
			var restBuf []byte
			for {
				var data ndtpData
				var packetLen uint16
				data, packetLen, restBuf, err = parseNDTP(restBuf)
				if err != nil {
					fmt.Println(err)
					break
				}
				if data.NPH.NPHReqID == 0 {
					restBuf = restBuf[packetLen:]
					continue
				}
				to.SetWriteDeadline(time.Now().Add(writeTimeout))
				_, err = to.Write(b[:packetLen])
				if err != nil {
					errClientCh <- err
					return
				}
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func client2servers(from net.Conn, ndtpConn *connection, ErrNDTPCh, errClientCh chan error, connNo uint64, mu *sync.Mutex) {
	var restBuf []byte
	for {
		var b []byte
		n, err := from.Read(b)
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
				fmt.Println(err)
				break
			}
			err = writeToDB(restBuf[:packetLen])
			if err != nil {
				errorRequest(from)
				restBuf = []byte{}
				break
			}
			if ndtpConn.closed != true {
				ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				_, err = ndtpConn.conn.Write(restBuf[:packetLen])
				if err != nil {
					ndtpConStatus(ndtpConn, mu, ErrNDTPCh)
				}
			}
			if egtsConn.closed != true {
				if toEGTS(data) {
					egtsCh <- data.ToRnis
				}
			}
			reply(from, restBuf[:packetLen])
			restBuf = restBuf[packetLen:]
		}
	}
}

func toEGTS(data ndtpData) bool {
	if data.ToRnis.Time != 0 {
		return true
	}
	return false
}

func ndtpConStatus(ndtpConn *connection, mu *sync.Mutex, ErrNDTPCh chan error) {
	mu.Lock()
	if ndtpConn.closed || ndtpConn.recon {
		return
	} else {
		ndtpConn.recon = true
		ndtpConn.conn.Close()
		ndtpConn.closed = true
		go reconnectNDTP(ndtpConn, ErrNDTPCh)
	}
	mu.Unlock()
}

func reconnectNDTP(ndtpConn *connection, ErrNDTPCh chan error) {
	for {
		if conClosed(ErrNDTPCh) {
			return
		}
		cN, err := net.Dial("tcp", NDTPAddress)
		if err != nil {
			log.Printf("error while connecting to server: %s", err)
		} else {
			//TODO pass id to function
			firstMessage := loadFirstMessage(0)
			ndtpConn.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			_, err := ndtpConn.conn.Write(firstMessage)
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
	egtsConn.conn.Close()
	egtsConn.closed = true
	cE, err := net.Dial("tcp", EGTSAddress)
	if err != nil {
		log.Printf("error while connecting to server: %s", err)
	} else {
		egtsConn.conn = cE
		egtsConn.closed = false
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

func reply(c net.Conn, packet []byte) {
	return
}

func errorRequest(c net.Conn) {
	return
}

func changeAddress(data []byte, ip net.IP) {
	start := []byte{0x7E, 0x7E}
	index1 := bytes.Index(data, start)
	for i, j := index1+9, 0; i < index1+13; i, j = i+1, j+1 {
		data[i] = ip[j]
	}
}

func getIP(c net.Conn) net.IP {
	ipPort := strings.Split(c.RemoteAddr().String(), ":")
	ip := ipPort[0]
	ip1 := net.ParseIP(ip)
	return ip1.To4()
}
