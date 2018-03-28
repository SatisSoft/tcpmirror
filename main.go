package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	defaultBufferSize = 1024
)

type mirror struct {
	addr   string
	conn   net.Conn
	closed bool
}

func readAndDiscard(m mirror, errChMirrors chan error) {
	for {
		var b [defaultBufferSize]byte
		_, err := m.conn.Read(b[:])
		if err != nil {
			m.conn.Close()
			m.closed = true
			errChMirrors <- err
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

		_, err = to.Write(b[:n])
		if err != nil {
			errChClient <- err
			return
		}
	}
}

func client2server(from net.Conn, to net.Conn, mirrors []mirror, errChServer, errChMirrors, errChClient chan error) {
	for {
		var b [defaultBufferSize]byte

		n, err := from.Read(b[:])
		if err != nil {
			errChClient <- err
			return
		}

		_, err = to.Write(b[:n])
		if err != nil {
			errChServer <- err
			return
		}

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

func connect(origin net.Conn, forwarder net.Conn, mirrors []mirror, errChServer, errChMirrors, errChClient chan error) {

	for i := 0; i < len(mirrors); i++ {
		go readAndDiscard(mirrors[i], errChMirrors)
	}

	go server2client(forwarder, origin, errChServer, errChClient)
	go client2server(origin, forwarder, mirrors, errChServer, errChMirrors, errChClient)

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

		go func(c net.Conn) {

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
					log.Printf("error while connecting to mirror %s: %s", addr, err)
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
			
			connect(c, cF, mirrors, errChServer, errChMirrors, errChClient)

			FORLOOP:
			for {
				select {
				case err := <-errChMirrors:
					log.Printf("error from mirror: %s", err)
				case err := <-errChClient:
					log.Printf("error from client: %s", err)
					break FORLOOP
				case err := <-errChServer:
					log.Printf("error from server: %s", err)
					break FORLOOP
				}
			}

			c.Close()
			cF.Close()

			for _, m := range mirrors {
				m.conn.Close()
			}
		}(c)

		connNo += 1
	}
}
