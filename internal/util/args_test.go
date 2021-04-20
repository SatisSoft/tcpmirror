package util

import (
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"
)

func Test_parseConfig(t *testing.T) {
	tests := []struct {
		name     string
		conf     string
		wantArgs *Args
		wantErr  bool
	}{
		{"config", "testconfig/example.toml", wantConfig(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotArgs, err := parseConfig(tt.conf)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("parseConfig() gotArgs = %v,\n 						"+
					"		want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

func wantConfig() *Args {
	vis1 := System{
		ID:       1,
		Address:  "10.20.30.41:5555",
		Protocol: "EGTS",
		IsMaster: false,
		Name:     "1_vis1_EGTS",
	}
	vis2 := System{
		ID:       2,
		Address:  "10.20.30.41:5556",
		Protocol: "NDTP",
		IsMaster: true,
		Name:     "2_vis2_NDTP",
	}
	vis3 := System{
		ID:       3,
		Address:  "10.20.30.41:5557",
		Protocol: "NDTP",
		IsMaster: false,
		Name:     "3_vis3_NDTP",
	}
	systems := []System{vis1, vis2, vis3}
	return &Args{
		Listen:     "localhost:8888",
		Protocol:   "NDTP",
		DB:         "localhost:8889",
		Monitoring: "localhost:9000",
		LogLevel:   logrus.TraceLevel,
		TestMode:   false,
		Systems:    systems,

		KeyEx:                20,
		TimeoutCloseSec:      5,
		TimeoutErrorReplySec: 5,
		TimeoutReconnectSec:  10,
		RedisMaxIdle:         220,
		RedisMaxActive:       220,

		PeriodNotConfDataEgtsMs:  2000,
		PeriodCheckOldEgtsMs:     30000,
		BatchOldEgts:             350,
		PeriodSendBatchOldEgtsMs: 100,
		WaitConfEgtsMs:           60000,

		PeriodNotConfDataNdtpMs: 2000,
		PeriodCheckOldNdtpMs:    30000,
		PeriodSendOnlyOldNdtpMs: 30000,
		WaitConfNdtpMs:          60000,
	}
}
