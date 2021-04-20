package monitoring

import (
	"time"

	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/cakturk/go-netstat/netstat"
	"github.com/egorban/influx/pkg/influx"
	"github.com/sirupsen/logrus"
)

const (
	SentBytes  = "sentBytes"
	RcvdBytes  = "rcvdBytes"
	SentPkts   = "sentPkts"
	RcvdPkts   = "rcvdPkts"
	SentRecs   = "sentRecs"
	RcvdRecs   = "rcvdRecs"
	QueuedPkts = "queuedPkts"
	QueuedRecs = "queuedRecs"
	RcvdConf   = "rcvdConf"

	numConns   = "numConns"
	unConfPkts = "unConfPkts"
)

func SendMetric(options *util.Options,
	table string,
	tags map[string]string,
	metricName string,
	value interface{}) {

	if !options.MonEnable {
		return
	}
	options.MonСlient.WritePoint(formPoint(table, tags, metricName, value))
}

func formPoint(table string,
	addedTags map[string]string,
	metricName string,
	value interface{}) *influx.Point {

	tags := influx.Tags{
		"host":     host,
		"instance": util.Instance,
	}

	for key, val := range addedTags {
		tags[key] = val
	}

	values := influx.Values{
		metricName: value,
	}
	return influx.NewPoint(table, tags, values)
}

func monSystemConns(monClient *influx.Client) {
	logrus.Println("start monitoring system connections with period:", periodMonSystemConns)
	for {
		time.Sleep(periodMonSystemConns)
		n, err := getSourceConns()
		if err == nil {
			monClient.WritePoint(formPoint(AttTable, map[string]string{"systemName": TerminalName}, numConns, n))
		}
		sysConns := make(map[string]int, len(systems))
		for _, sys := range systems {
			n, err := getSystemConns(sys)
			if err == nil {
				sysConns[sys.name] = n
			}
		}
		if len(sysConns) > 0 {
			for name, count := range sysConns {
				monClient.WritePoint(formPoint(VisTable, map[string]string{"systemName": name}, numConns, count))
			}
		}
	}
}

func getSourceConns() (n int, err error) {
	tabsTcp, err := netstat.TCPSocks(func(s *netstat.SockTabEntry) bool {
		return s.State == netstat.Established && s.LocalAddr.Port == listenPort
	})
	if err != nil {
		logrus.Println("error get source connections:", err)
	}
	tabsTcp6, err := netstat.TCP6Socks(func(s *netstat.SockTabEntry) bool {
		return s.State == netstat.Established && s.LocalAddr.Port == listenPort
	})
	if err != nil {
		logrus.Println("error get source connections:", err)
	}
	n = len(tabsTcp) + len(tabsTcp6)
	return
}

func getSystemConns(sys sysInfo) (n int, err error) {
	tabsTcp, err := netstat.TCPSocks(func(s *netstat.SockTabEntry) bool {
		return s.State == netstat.Established &&
			s.RemoteAddr.IP.String() == sys.ipAddress &&
			s.RemoteAddr.Port == sys.port
	})
	if err != nil {
		logrus.Println("error get source connections:", err)
	}
	tabsTcp6, err := netstat.TCP6Socks(func(s *netstat.SockTabEntry) bool {
		return s.State == netstat.Established &&
			s.RemoteAddr.IP.String() == sys.ipAddress &&
			s.RemoteAddr.Port == sys.port
	})
	if err != nil {
		logrus.Println("error get source connections:", err)
	}
	if tabsTcp != nil && tabsTcp6 != nil {
		tabs := append(tabsTcp, tabsTcp6...)
		for _, e := range tabs {
			if e.Process != nil {
				if e.Process.Pid == pidInstance {
					n = n + 1
				}
			}
		}
	}
	return
}
