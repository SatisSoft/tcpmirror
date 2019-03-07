package main

import (
	"github.com/ashirko/go-metrics"
	"github.com/ashirko/go-metrics-graphite"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/sirupsen/logrus"
	"net"
	"time"
)

func startMetrics(graphiteAddress string) {
	addr, err := net.ResolveTCPAddr("tcp", graphiteAddress)
	if err != nil {
		logrus.Errorf("error while connection to graphite: %s\n", err)
	} else {
		registerMetric("clNDTP", metrics.NewCustomCounter())
		registerMetric("toServNDTP", metrics.NewCustomCounter())
		registerMetric("fromServNDTP", metrics.NewCustomCounter())
		registerMetric("servEGTS", metrics.NewCustomCounter())
		registerMetric("memFree", metrics.NewGauge())
		registerMetric("memUsed", metrics.NewGauge())
		registerMetric("UsedPercent", metrics.NewGaugeFloat64())
		registerMetric("cpu15", metrics.NewGaugeFloat64())
		registerMetric("cpu1", metrics.NewGaugeFloat64())
		enableMetrics = true
		logrus.Println("start sending metrics to graphite")
		go graphite.Graphite(metrics.DefaultRegistry, 10*10e8, "ndtpserv.metrics", addr)
		go periodicSysMon()
	}
	return
}

func registerMetric(name string, metric interface{}){
	err := metrics.Register(name, metric)
	if err != nil {
		logrus.Errorf("can't registe metric %s : %s", name, err)
	}
	return
}

func periodicSysMon() {
	for {
		v, err := mem.VirtualMemory()
		if err != nil {
			logrus.Errorf("periodic mem mon error: %s", err)
		} else {
			memFree.Update(int64(v.Free))
			memUsed.Update(int64(v.Used))
			usedPercent.Update(v.UsedPercent)
		}
		c, err := load.Avg()
		if err != nil {
			logrus.Errorf("periodic cpu mon error: %s", err)
		} else {
			cpu1.Update(c.Load1)
			cpu15.Update(c.Load15)
		}
		time.Sleep(10 * time.Second)
	}
}
