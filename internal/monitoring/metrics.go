package monitoring

import (
	"github.com/ashirko/go-metrics"
	"github.com/ashirko/tcpmirror/internal/util"
	graphite "github.com/egorban/go-metrics-graphite"
	"github.com/sirupsen/logrus"
	"net"
)

func Init(address string) (enable bool, err error) {
	if address == "" {
		logrus.Println("start without sending metrics to graphite")
		return
	}
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		logrus.Errorf("error while connecting to graphite: %s\n", err)
		return
	}
	regitsterMetrics()
	logrus.Infof("start sending metrics to graphite to %+s, prefix: %+s",
    	    address, util.InstancePrefix)
	go graphite.Graphite(metrics.DefaultRegistry, 3e10, util.InstancePrefix, addr)
	go periodicSysMon()
	return true, nil
}

func regitsterMetrics() {
	countClientNDTP = metrics.NewCustomCounter()
	countToServerNDTP = metrics.NewCustomCounter()
	countFromServerNDTP = metrics.NewCustomCounter()
	countServerEGTS = metrics.NewCustomCounter()
	memFree = metrics.NewGauge()
	memUsed = metrics.NewGauge()
	cpu15 = metrics.NewGaugeFloat64()
	cpu1 = metrics.NewGaugeFloat64()
	usedPercent = metrics.NewGaugeFloat64()
	registerMetric("clNDTP", countClientNDTP)
	registerMetric("toServNDTP", countToServerNDTP)
	registerMetric("fromServNDTP", countFromServerNDTP)
	registerMetric("servEGTS", countServerEGTS)
	registerMetric("memFree", memFree)
	registerMetric("memUsed", memUsed)
	registerMetric("UsedPercent", usedPercent)
	registerMetric("cpu15", cpu15)
	registerMetric("cpu1", cpu1)
	EnableMetrics = true
}

func registerMetric(name string, metric interface{}) {
	err := metrics.Register(name, metric)
	if err != nil {
		logrus.Errorf("can't registe metric %s : %s", name, err)
	}
	return
}
