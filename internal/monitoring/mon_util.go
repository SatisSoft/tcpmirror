package monitoring

import (
	"net"
	"os"
	"strconv"

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

func getHost() (hostName string) {
	hostName = "localhost"
	hostName, err := os.Hostname()
	if err != nil {
		logrus.Println("error get hostname", err)
	}
	return
}
