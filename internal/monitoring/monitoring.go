package monitoring

import (
	"time"

	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/egorban/influx/pkg/influx"
	"github.com/sirupsen/logrus"
)

const (
	TerminalName = "terminal"

	visTable   = "vis"
	attTable   = "source"
	redisTable = "redis"

	periodMonSystemConns     = 10 * time.Second
	periodMonRedisConns      = 60 * time.Second
	periodMonRedisUnConfPkts = 60 * time.Second
)

var (
	host      string
	dbAddress string
	dbPort    uint16
)

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
	startRedisPeriodicMon(monClient, args)
	startSystemsPeriodicMon(monClient, args)
	return
}
func startSystemsPeriodicMon(monClient *influx.Client, args *util.Args) {
	systems := args.Systems
	if systems == nil {
		logrus.Println("start without monitoring system connections")
		return
	}

	sysConns.muNums.Lock()
	defer sysConns.muNums.Unlock()
	sysConns.nums = make(map[string]uint64, len(systems)+1)
	sysConns.nums[TerminalName] = 0
	for _, sys := range systems {
		sysConns.nums[sys.Name] = 0
	}

	go monSystemConns(monClient)
}

func startRedisPeriodicMon(monClient *influx.Client, args *util.Args) {
	dbAddress = args.DB
	if dbAddress == "" {
		logrus.Println("start without monitoring redis")
		return
	}

	go monRedisPkts(monClient)

	go func() {
		dbPort = getRedisPort()
		if dbPort == 0 {
			logrus.Println("start without monitoring redis connections")
			return
		}
		monRedisConns(monClient)
	}()
}