package monitoring

import (
	"bytes"
	"time"

	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/cakturk/go-netstat/netstat"
	"github.com/egorban/influx/pkg/influx"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
)

// type systemConns struct {
// 	nums   map[string]uint64
// 	muNums sync.Mutex
// }

const (
	SentBytes  = "sentBytes"
	RcvdBytes  = "rcvdBytes"
	SentPkts   = "sentPkts"
	RcvdPkts   = "rcvdPkts"
	QueuedPkts = "queuedPkts"

	numConns   = "numConns"
	unConfPkts = "unConfPkts"
)

func SendMetric(options *util.Options, systemName string, metricName string, value interface{}) {
	if !options.MonEnable {
		return
	}
	options.MonСlient.WritePoint(formPoint(systemName, metricName, value))
}

func monSystemConns(monClient *influx.Client, systems []sysInfo) {
	logrus.Println("start monitoring system connections with period:", periodMonSystemConns)
	for {
		time.Sleep(periodMonSystemConns)
		n, err := getSourceConns()
		if err == nil {
			monClient.WritePoint(formPoint(TerminalName, numConns, n))
		}
		for _, sys := range systems {
			n, _ := getSystemConns(sys)
			monClient.WritePoint(formPoint(sys.name, numConns, n))
		}
	}
}

func getSourceConns() (n int, err error) {
	tabs, err := netstat.TCPSocks(func(s *netstat.SockTabEntry) bool {
		return s.State == netstat.Established && s.LocalAddr.Port == listenPort
	})
	if err != nil {
		logrus.Println("error get source connections:", err)
		return
	}
	n = len(tabs)
	return
}

func getSystemConns(sys sysInfo) (n int, err error) {
	tabs, err := netstat.TCPSocks(func(s *netstat.SockTabEntry) bool {
		return s.State == netstat.Established &&
			s.RemoteAddr.IP.String() == sys.ipAddress &&
			s.RemoteAddr.Port == sys.port
	})
	if err != nil {
		logrus.Println("error get source connections:", err)
		return
	}

	for _, e := range tabs {
		if e.Process != nil {
			if e.Process.Pid == pidInstance {
				n = n + 1
			}
		}
	}
	return
}

func monRedisPkts(monClient *influx.Client) {
	logrus.Println("start monitoring redis connections with period:", periodMonRedisUnConfPkts)
	for {
		time.Sleep(periodMonRedisUnConfPkts)
		conn := db.Connect(dbAddress)
		defer db.Connect(dbAddress)
		if conn == nil {
			logrus.Println("error monitoring redis unconfirmed packets: conn=nil")
			continue
		}
		n, err := redis.Uint64(conn.Do("ZCOUNT", util.EgtsName, 0, util.Milliseconds()))
		if err != nil {
			logrus.Println("error get egts unconfirmed packets: ", err)
			continue
		}
		sessions, err := redis.ByteSlices(conn.Do("KEYS", "session:*"))
		if err != nil {
			logrus.Println("error error get ndtp sessions: ", err)
		} else {
			prefix := []byte("session:")
			for _, s := range sessions {
				id := bytes.TrimPrefix(s, prefix)
				nNdtp, err := redis.Uint64(conn.Do("ZCOUNT", id, 0, util.Milliseconds()))
				if err != nil {
					logrus.Println("error get ndtp unconfirmed packets: ", err)
					continue
				}
				n = n + nNdtp
			}
		}
		monClient.WritePoint(formPoint(redisTable, unConfPkts, n))
	}
}

func monRedisConns(monClient *influx.Client) {
	logrus.Println("start monitoring redis unconfirmed packets with period:", periodMonRedisConns)
	for {
		time.Sleep(periodMonRedisConns)
		tabs, err := netstat.TCPSocks(func(s *netstat.SockTabEntry) bool {
			return s.State == netstat.Established && s.LocalAddr.Port == dbPort
		})
		if err != nil {
			logrus.Println("error get redis connections:", err)
			continue
		}
		monClient.WritePoint(formPoint(redisTable, numConns, len(tabs)))
	}
}

func formPoint(systemName string, metricName string, value interface{}) *influx.Point {
	table := getTable(systemName)
	tags := influx.Tags{
		"host":     host,
		"instance": util.Instance,
	}
	if table == visTable {
		tags["system"] = systemName
	}
	values := influx.Values{
		metricName: value,
	}
	return influx.NewPoint(table, tags, values)
}
