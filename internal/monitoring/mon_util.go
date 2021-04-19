package monitoring

import (
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

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
	portStr := strings.TrimPrefix(listenAddress, ":")
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

func GetDefaultMonTags(defTags map[string]string) (monTags map[string]string) {
	monTags = make(map[string]string)
	for k, v := range defTags {
		monTags[k] = v
	}
	return
}
