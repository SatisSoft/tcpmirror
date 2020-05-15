package monitoring

import (
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

func getTable(systemName string) string {
	switch systemName {
	case TerminalName:
		return attTable
	default:
		return visTable
	}
}

func splitAddrPort(address string) (addr string, port uint16, err error) {
	addr, portStr, err := net.SplitHostPort(address)
	if err != nil || portStr == "" || addr == "" {
		logrus.Println("error split addr and port: ", err)
		return
	}
	portInt, err := strconv.Atoi(portStr)
	if err != nil {
		logrus.Println("error split addr and port: ", err)
		return
	}
	port = uint16(portInt)
	return
}

func getListenPort(listenAddress string) (port uint16) {
	portStr := strings.TrimPrefix(":", listenAddress)
	log.Println("DEBUG portStr", portStr)
	portInt, err := strconv.Atoi(portStr)
	if err != nil {
		logrus.Println("error get port: ", err)
		return
	}
	port = uint16(portInt)
	return
}

func getHost() (hostName string) {
	hostName = "localhost"
	hostName, err := os.Hostname()
	if err != nil {
		logrus.Println("error get hostname", err)
	}
	return
}
