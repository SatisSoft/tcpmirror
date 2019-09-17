package test

import (
	"flag"
	"github.com/ashirko/tcpmirror/internal/server"
	"github.com/sirupsen/logrus"
	"os"
	"testing"
)

func Test_serverStart(t *testing.T) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	//logrus.Tracef("flags0: %v", os.Args[:])
	err := flag.Set("conf", "../config/example.toml")
	logrus.Tracef("err: %v", err)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Tracef("flags1: %v", os.Args[:])
	server.Start()
}
