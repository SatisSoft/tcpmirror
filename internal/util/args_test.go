package util

import (
	"reflect"
	"testing"
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
	}
	vis2 := System{
		ID:       2,
		Address:  "10.20.30.41:5556",
		Protocol: "NDTP",
		IsMaster: true,
	}
	vis3 := System{
		ID:       3,
		Address:  "10.20.30.41:5557",
		Protocol: "NDTP",
		IsMaster: false,
	}
	systems := []System{vis1, vis2, vis3}
	return &Args{
		Listen:     "localhost:8888",
		Systems:    systems,
		Monitoring: "localhost:9000",
		DB:         "localhost:8889",
	}
}
