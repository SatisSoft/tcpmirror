package monitoring

import (
	"os"
	"time"

	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/egorban/influx/pkg/influx"
	"github.com/sirupsen/logrus"
)

const (
	TerminalName = "terminal"
	SourceName   = "sourceEgts"

	visTable = "vis"
	attTable = "source"

	periodMonSystemConns = 10 * time.Second
)

var (
	host        string
	listenPort  uint16
	pidInstance int
	systems     []sysInfo
)

type sysInfo struct {
	name      string
	ipAddress string
	port      uint16
}

func Init(args *util.Args) (monEnable bool, monClient *influx.Client, err error) {
	monAddress := args.Monitoring
	if monAddress == "" {
		logrus.Println("start without sending metrics to influx")
		return
	}
	monClient, err = influx.NewClient(monAddress)
	if err != nil {
		logrus.Println("error while connecting to influx", err)
		return
	}
	host = getHost()
	monEnable = true
	startSystemsPeriodicMon(monClient, args)
	return
}
func startSystemsPeriodicMon(monClient *influx.Client, args *util.Args) {
	if args.Systems == nil {
		logrus.Println("error get systems, start without monitoring system connections")
		return
	}
	systems = make([]sysInfo, 0)
	for _, sys := range args.Systems {
		ipAddress, port, err := splitAddrPort(sys.Address)
		if err != nil {
			logrus.Println("error get systems, start without monitoring system connections", err)
			return
		}
		if ipAddress == "localhost" {
			ipAddress = "127.0.0.1"
		}
		system := sysInfo{
			name:      sys.Name,
			ipAddress: ipAddress,
			port:      port,
		}
		systems = append(systems, system)
	}

	listenAddress := args.Listen
	if listenAddress == "" {
		logrus.Println("error get listen address, start without monitoring redis")
		return
	}
	listenPort = getListenPort(listenAddress)

	pidInstance = os.Getpid()
	if pidInstance == 0 {
		logrus.Println("error get pid, start without monitoring system connections")
		return
	}
	go monSystemConns(monClient)
}
