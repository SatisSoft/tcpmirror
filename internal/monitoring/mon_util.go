package monitoring

import (
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
	case redisTable:
		return redisTable
	default:
		return visTable
	}
}

func getRedisPort() (dbPort uint16) {
	_, dbPortStr, err := net.SplitHostPort(dbAddress)
	if err != nil || dbPortStr == "" {
		logrus.Println("error get redis port: ", err, dbPortStr)
		return
	}
	dbPortInt, err := strconv.Atoi(dbPortStr)
	if err != nil {
		logrus.Println("error get redis port: ", err)
		return
	}
	return uint16(dbPortInt)
}

func getHost() (ipStr string) {
	ipStr = "localhost"
	host, err := os.Hostname()
	if err != nil {
		return
	}
	addrs, err := net.LookupIP(host)
	if err != nil {
		return
	}
	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			logrus.Println("IPv4: ", ipv4)
			ipStr = strings.ReplaceAll(ipv4.String(), ".", "_")
		}
	}
	return
}
