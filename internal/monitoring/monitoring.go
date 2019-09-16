package monitoring

import (
	"github.com/ashirko/go-metrics"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/sirupsen/logrus"
	"time"
)

var (
	countClientNDTP     metrics.Counter
	countToServerNDTP   metrics.Counter
	countFromServerNDTP metrics.Counter
	countServerEGTS     metrics.Counter
	memFree             metrics.Gauge
	memUsed             metrics.Gauge
	cpu15               metrics.GaugeFloat64
	cpu1                metrics.GaugeFloat64
	usedPercent         metrics.GaugeFloat64
	EnableMetrics       bool
)

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
