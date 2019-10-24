package server

import (
	"errors"
	"github.com/ashirko/tcpmirror/internal/client"
	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/monitoring"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

const (
	defaultBufferSize = 1024
	writeTimeout      = 10 * time.Second
	readTimeout       = 180 * time.Second
)

// Start server
func Start() {
	args, err := util.ParseArgs()
	if err != nil {
		logrus.Error("can't parse arguments:", err)
		os.Exit(1)
	}
	options, err := initialize(args)
	logrus.Infof("start with args %+v and options %+v", args, options)
	if err != nil {
		logrus.Error("can't init server:", err)
		os.Exit(1)
	}
	egtsClients, err := initEgtsClients(options, args)
	if err != nil {
		logrus.Error("can't init clients:", err)
		os.Exit(1)
	}
	startClients(egtsClients)
	startServer(args, options, egtsClients)
}

func initialize(args *util.Args) (options *util.Options, err error) {
	logrus.SetReportCaller(true)
	logrus.SetLevel(args.LogLevel)
	options = new(util.Options)
	options.Mon, err = monitoring.Init(args.Monitoring)
	if err != nil {
		return
	}
	options.DB = args.DB
	if options.DB == "" {
		err = errors.New("no DB server address")
	}
	db.SysNumber = len(args.Systems)
	options.KeyEx = args.KeyEx
	options.PeriodNotConfData = args.PeriodNotConfData
	options.PeriodOldData = args.PeriodOldData
	options.PeriodCheckOld = args.PeriodCheckOld
	options.TimeoutClose = args.TimeoutClose
	options.TimeoutErrorReply = options.TimeoutErrorReply
	options.TimeoutReconnect = options.TimeoutReconnect
	return
}

func initEgtsClients(options *util.Options, args *util.Args) (egtsClients []client.Client, err error) {
	for _, sys := range args.Systems {
		switch sys.Protocol {
		case "EGTS":
			c := client.NewEgts(sys, options)
			egtsClients = append(egtsClients, c)
		default:
			continue
		}
	}
	return
}

func startServer(args *util.Args, options *util.Options, egtsClients []client.Client) {
	listen := args.Listen
	logrus.Tracef("egts clients: %v", egtsClients)
	channels := inputChanels(egtsClients)
	logrus.Tracef("input channels: %v", channels)
	switch args.Protocol {
	case "NDTP":
		startNdtpServer(listen, options, channels, args.Systems)
	default:
		logrus.Errorf("undefined server protocol: %s", args.Protocol)
	}
}

func startClients(clients []client.Client) {
	logrus.Tracef("start clients: %v", clients)
	for _, c := range clients {
		go client.Start(c)
	}
}

func inputChanels(clients []client.Client) []chan []byte {
	var channels []chan []byte
	for _, c := range clients {
		channels = append(channels, c.InputChannel())
	}
	return channels
}
