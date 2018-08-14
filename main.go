package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

const (
	defaultBufferSize = 1024
	writeTimeout      = 10 * time.Second
)

type mirror struct {
	addr   string
	conn   net.Conn
	closed bool
	recon  bool
}

func mirror2null(m *mirror, errChMirrors chan error) {
	for {
		var b [defaultBufferSize]byte
		_, err := (*m).conn.Read(b[:])
		if err != nil && !(*m).closed {
			(*m).conn.Close()
			(*m).closed = true
			errChMirrors <- err
			return
		}
		if (*m).closed {
			return
		}
	}
}

func server2client(from net.Conn, to net.Conn, errChServer, errChClient chan error) {
	for {
		var b [defaultBufferSize]byte

		n, err := from.Read(b[:])
		if err != nil {
			errChServer <- err
			return
		}
		to.SetWriteDeadline(time.Now().Add(writeTimeout))
		_, err = to.Write(b[:n])
		if err != nil {
			errChClient <- err
			return
		}
	}
}

func client2server(from net.Conn, to net.Conn, mirrors []mirror, errChServer, errChMirrors, errChClient chan error, connNo uint64) {
	for {
		var b [defaultBufferSize]byte

		n, err := from.Read(b[:])
		if err != nil {
			errChClient <- err
			return
		}
		//TODO remove after testing
		log.Printf("connNo %d: read %d bytes from client", connNo, n)

		to.SetWriteDeadline(time.Now().Add(writeTimeout))
		_, err = to.Write(b[:n])
		if err != nil {
			errChServer <- err
			return
		}
		log.Printf("connNo %d: write %d bytes to server", connNo, n)

		for i := 0; i < len(mirrors); i++ {
			if mirrors[i].closed {
				log.Printf("don't send message to closed mirror conn")
				continue
			}
			_, err = mirrors[i].conn.Write(b[:n])
			if err != nil {
				mirrors[i].conn.Close()
				mirrors[i].closed = true
				errChMirrors <- err
			}
		}
	}
}

func connect(origin net.Conn, forwarder net.Conn, mirrors []mirror, errChServer, errChMirrors, errChClient chan error, connNo uint64) {
	for i := 0; i < len(mirrors); i++ {
		go mirror2null(&(mirrors[i]), errChMirrors)
	}
	go server2client(forwarder, origin, errChServer, errChClient)
	go client2server(origin, forwarder, mirrors, errChServer, errChMirrors, errChClient, connNo)
}

type mirrorList []string

func (l *mirrorList) String() string {
	return fmt.Sprint(*l)
}

func (l *mirrorList) Set(value string) error {
	for _, m := range strings.Split(value, ",") {
		*l = append(*l, m)
	}
	return nil
}

func main() {

	var (
		listenAddress   string
		forwardAddress  string
		mirrorAddresses mirrorList
		//mu sync.Mutex
	)

	flag.StringVar(&listenAddress, "l", "", "listen address (e.g. 'localhost:8080')")
	flag.StringVar(&forwardAddress, "f", "", "forward to address (e.g. 'localhost:8081')")
	flag.Var(&mirrorAddresses, "m", "comma separated list of mirror addresses (e.g. 'localhost:8082,localhost:8083')")
	flag.Parse()

	if listenAddress == "" || forwardAddress == "" {
		flag.Usage()
		return
	}

	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("error while listening: %s", err)
	}

	connNo := uint64(1)

	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatalf("error while accepting: %s", err)
		}

		log.Printf("accepted connection %d (%s <-> %s)", connNo, c.RemoteAddr(), c.LocalAddr())

		go func(c net.Conn, connNo uint64) {

			cF, err := net.Dial("tcp", forwardAddress)
			if err != nil {
				log.Printf("error while connecting to server: %s", err)
				c.Close()
				return
			}

			var mirrors []mirror

			for _, addr := range mirrorAddresses {
				c, err := net.Dial("tcp", addr)
				if err != nil {
					log.Printf("error while connecting to mirror to servers %s: %s", addr, err)
				} else {
					mirrors = append(mirrors, mirror{
						addr:   addr,
						conn:   c,
						closed: false,
					})
				}

			}

			errChServer := make(chan error)
			errChMirrors := make(chan error)
			errChClient := make(chan error)

			connect(c, cF, mirrors, errChServer, errChMirrors, errChClient, connNo)

		FORLOOP:
			for {
				select {
				case err := <-errChMirrors:
					log.Printf("%d error from mirror: %s", connNo, err)
				case err := <-errChClient:
					log.Printf("%d error from client: %s", connNo, err)
					break FORLOOP
				case err := <-errChServer:
					log.Printf("%d error from server: %s", connNo, err)
					break FORLOOP
				}
			}
			c.Close()
			cF.Close()
			for _, m := range mirrors {
				m.conn.Close()
			}

		}(c, connNo)

		connNo += 1
	}
}
