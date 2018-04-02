package main

import (
	"flag"
	"time"
	"fmt"
	"log"
	"net"
	"strings"
	"bytes"
	"encoding/binary"
)

const (
	defaultBufferSize = 1024
	headerSize = 15
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
		if (err != nil && !(*m).closed) {
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

		_, err = to.Write(b[:n])
		if err != nil {
			errChClient <- err
			return
		}
	}
}

func client2server(from net.Conn, to net.Conn, mirrors []mirror, errChServer, errChMirrors, errChClient chan error, firstMesMir []byte) {
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
				if !mirrors[i].recon {
					mirrors[i].recon = true
					go func(m *mirror, errChMirrors chan error) {
						c, err := net.Dial("tcp", (*m).addr)
						if err != nil {
							log.Printf("error reconnect to mirror")
							time.Sleep(60 * time.Second)
						} else {
							(*m).conn = c
							(*m).closed = false
							firstLen := len(firstMesMir)
							_, err = mirrors[i].conn.Write(firstMesMir[:firstLen])
							if err != nil {
								mirrors[i].conn.Close()
								mirrors[i].closed = true
								errChMirrors <- err
							}
							_, err = mirrors[i].conn.Write(b[:n])
							if err != nil {
								mirrors[i].conn.Close()
								mirrors[i].closed = true
								errChMirrors <- err
							}
							go mirror2null(m, errChMirrors)
						}
						(*m).recon = false
					}(&(mirrors[i]), errChMirrors)
				}
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

func send_first_message(from net.Conn, to net.Conn, mirrors []mirror, firstMessage, firstMesMir []byte) bool {
	n := len(firstMessage)
	_, err := to.Write(firstMessage[:n])
	if err != nil {
		return false
	}

	for i := 0; i < len(mirrors); i++ {
		_, err = mirrors[i].conn.Write(firstMesMir[:n])
		if err != nil {
			mirrors[i].conn.Close()
			mirrors[i].closed = true
		}
	}
	return true
}



func connect(origin net.Conn, forwarder net.Conn, mirrors []mirror, errChServer, errChMirrors, errChClient chan error, firstMesMir []byte) {
		for i := 0; i < len(mirrors); i++ {
			go mirror2null(&(mirrors[i]), errChMirrors)
		}
		go server2client(forwarder, origin, errChServer, errChClient)
		go client2server(origin, forwarder, mirrors, errChServer, errChMirrors, errChClient, firstMesMir)
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
			//get first message
			var b [defaultBufferSize]byte
			n, err := c.Read(b[:])
			if err != nil {
				log.Printf("%d error while getting first message from client %s", connNo, c.RemoteAddr())
				c.Close()
				return
			}
			log.Printf("%d got first message from client %s", connNo, c.RemoteAddr())
			firstMessage := b[:n]
			log.Printf("%d receive message %x ", connNo, firstMessage)
			//parse first message
			if !verificate_message(firstMessage, n){
				log.Printf("error: first message is incorrect")
				c.Close()
				return
			}
			firstMesMir := make([]byte, n)
			copy(firstMesMir, firstMessage)
			ip := get_ip(c)
			log.Printf("conn %d: ip: %s\n", connNo, ip)
			//TODO remove from prod after testing
			log.Printf("before change %x ", firstMessage)
			change_address(firstMessage, ip)
			//TODO remove from prod after testing
			log.Printf("after change %x ", firstMessage)

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
						recon:  false,
					})
				}
			}

			errChServer := make(chan error)
			errChMirrors := make(chan error)
			errChClient := make(chan error)


			if !send_first_message(c, cF, mirrors, firstMessage, firstMesMir){
				log.Printf("%d error while sending first message %s", connNo, ip)
				c.Close()
				cF.Close()
				for _, m := range mirrors {
					m.conn.Close()
				}
			} else{
				log.Printf("%d sended first message %s", connNo, ip)

				connect(c, cF, mirrors, errChServer, errChMirrors, errChClient, firstMesMir)

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
			}
		}(c, connNo)

		connNo += 1
	}
}

func change_address(data []byte, ip net.IP){
	start := []byte{0x7E, 0x7E}
	index1 := bytes.Index(data, start)
	for i,j := index1+31,0 ; i<35; i,j = i+1, j+1{
		data[i] = ip[j]
	}
	lenData1 := binary.LittleEndian.Uint16(data[index1+2:index1+4])
	newCrc := crc16(data[index1+headerSize:index1+headerSize+int(lenData1)])
	nn := make([]byte, 2)
	binary.BigEndian.PutUint16(nn,newCrc)
	log.Printf("%T %x\n", nn, nn)
	for i,j := index1+6,0 ; i<8; i,j = i+1, j+1{
		data[i] = nn[j]
	}
}

func get_ip(c net.Conn) net.IP{
	ipPort := strings.Split(c.RemoteAddr().String(), ":")
	ip := ipPort[0]
	ip1 := net.ParseIP(ip)
	return ip1.To4()
}

func verificate_message(data []byte, lenData int) bool{
	//check start symbols
	start := []byte{0x7E, 0x7E}
	index1 := bytes.Index(data, start)
	//TODO remove from prod after testing
	log.Printf("index1: %d\n", index1)
	if index1 == -1{
		log.Println("error: start symbols not found")
		return false
	}
	//check packet size > header size
	if lenData <= headerSize{
		log.Println("error: packet length is smaller then header size")
		return false
	}
	//check packet size > data field size
	lenData1 := binary.LittleEndian.Uint16(data[index1+2:index1+4])
	if lenData1 >= uint16(lenData){
		log.Println("error: packet length is smaller then data field size")
		return false
	}
	//check crc if crc_flag == true
	if binary.LittleEndian.Uint16(data[index1+4:index1+6]) & 2 != 0{
		log.Println("need to check crc")
		crcHead := binary.BigEndian.Uint16(data[index1+6:index1+8])
		crcCalc := crc16(data[index1+headerSize:index1+headerSize+int(lenData1)])
		//TODO remove from prod after testing
		log.Printf("Crc header: %x; Crc calc: %x\n", crcHead, crcCalc)
		if crcHead != crcCalc {
			log.Println("error: crc incorrect")
			return false
		}
	} else {
		log.Println("don't need to check crc")
	}
	//check service_id == NPH_SRV_GENERIC_CONTROLS and type == NPH_SGC_CONN_REQUEST
	if binary.LittleEndian.Uint16(data[index1+15:index1+17]) != 0 || binary.LittleEndian.Uint16(data[index1+17:index1+19]) != 100{
		log.Println("error: this is not CONN_REQUEST packet")
		return false
	}
	return true
}

func crc16(bs []byte) (crc uint16) {
	l := len(bs)
	crc = 0xFFFF
	for i := 0; i < l; i++ {
		crc = (crc >> 8) ^ crc16tab[(crc&0xff)^uint16(bs[i])]
	}
	return
}

var crc16tab = [256]uint16{
	0x0000, 0xC0C1, 0xC181, 0x0140, 0xC301, 0x03C0, 0x0280, 0xC241,
	0xC601, 0x06C0, 0x0780, 0xC741, 0x0500, 0xC5C1, 0xC481, 0x0440,
	0xCC01, 0x0CC0, 0x0D80, 0xCD41, 0x0F00, 0xCFC1, 0xCE81, 0x0E40,
	0x0A00, 0xCAC1, 0xCB81, 0x0B40, 0xC901, 0x09C0, 0x0880, 0xC841,
	0xD801, 0x18C0, 0x1980, 0xD941, 0x1B00, 0xDBC1, 0xDA81, 0x1A40,
	0x1E00, 0xDEC1, 0xDF81, 0x1F40, 0xDD01, 0x1DC0, 0x1C80, 0xDC41,
	0x1400, 0xD4C1, 0xD581, 0x1540, 0xD701, 0x17C0, 0x1680, 0xD641,
	0xD201, 0x12C0, 0x1380, 0xD341, 0x1100, 0xD1C1, 0xD081, 0x1040,
	0xF001, 0x30C0, 0x3180, 0xF141, 0x3300, 0xF3C1, 0xF281, 0x3240,
	0x3600, 0xF6C1, 0xF781, 0x3740, 0xF501, 0x35C0, 0x3480, 0xF441,
	0x3C00, 0xFCC1, 0xFD81, 0x3D40, 0xFF01, 0x3FC0, 0x3E80, 0xFE41,
	0xFA01, 0x3AC0, 0x3B80, 0xFB41, 0x3900, 0xF9C1, 0xF881, 0x3840,
	0x2800, 0xE8C1, 0xE981, 0x2940, 0xEB01, 0x2BC0, 0x2A80, 0xEA41,
	0xEE01, 0x2EC0, 0x2F80, 0xEF41, 0x2D00, 0xEDC1, 0xEC81, 0x2C40,
	0xE401, 0x24C0, 0x2580, 0xE541, 0x2700, 0xE7C1, 0xE681, 0x2640,
	0x2200, 0xE2C1, 0xE381, 0x2340, 0xE101, 0x21C0, 0x2080, 0xE041,
	0xA001, 0x60C0, 0x6180, 0xA141, 0x6300, 0xA3C1, 0xA281, 0x6240,
	0x6600, 0xA6C1, 0xA781, 0x6740, 0xA501, 0x65C0, 0x6480, 0xA441,
	0x6C00, 0xACC1, 0xAD81, 0x6D40, 0xAF01, 0x6FC0, 0x6E80, 0xAE41,
	0xAA01, 0x6AC0, 0x6B80, 0xAB41, 0x6900, 0xA9C1, 0xA881, 0x6840,
	0x7800, 0xB8C1, 0xB981, 0x7940, 0xBB01, 0x7BC0, 0x7A80, 0xBA41,
	0xBE01, 0x7EC0, 0x7F80, 0xBF41, 0x7D00, 0xBDC1, 0xBC81, 0x7C40,
	0xB401, 0x74C0, 0x7580, 0xB541, 0x7700, 0xB7C1, 0xB681, 0x7640,
	0x7200, 0xB2C1, 0xB381, 0x7340, 0xB101, 0x71C0, 0x7080, 0xB041,
	0x5000, 0x90C1, 0x9181, 0x5140, 0x9301, 0x53C0, 0x5280, 0x9241,
	0x9601, 0x56C0, 0x5780, 0x9741, 0x5500, 0x95C1, 0x9481, 0x5440,
	0x9C01, 0x5CC0, 0x5D80, 0x9D41, 0x5F00, 0x9FC1, 0x9E81, 0x5E40,
	0x5A00, 0x9AC1, 0x9B81, 0x5B40, 0x9901, 0x59C0, 0x5880, 0x9841,
	0x8801, 0x48C0, 0x4980, 0x8941, 0x4B00, 0x8BC1, 0x8A81, 0x4A40,
	0x4E00, 0x8EC1, 0x8F81, 0x4F40, 0x8D01, 0x4DC0, 0x4C80, 0x8C41,
	0x4400, 0x84C1, 0x8581, 0x4540, 0x8701, 0x47C0, 0x4680, 0x8641,
	0x8201, 0x42C0, 0x4380, 0x8341, 0x4100, 0x81C1, 0x8081, 0x4040}
