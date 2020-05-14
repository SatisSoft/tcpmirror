package server

import (
	"errors"
	"os"
	"time"

	"github.com/ashirko/tcpmirror/internal/client"
	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/ashirko/tcpmirror/internal/monitoring"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
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
	deleteManager := initDeleteManager(options, args)
	egtsClients, err := initEgtsClients(options, args, deleteManager.Chan)
	if err != nil {
		logrus.Error("can't init clients:", err)
		os.Exit(1)
	}
	startClients(egtsClients)
	startServer(args, options, egtsClients, deleteManager.Chan)
}

func initDeleteManager(options *util.Options, args *util.Args) *db.DeleteManager {
	systemIds := getSystemIds(args.Systems)
	return db.InitDeleteManager(options.DB, systemIds)
}

func initialize(args *util.Args) (options *util.Options, err error) {
	logrus.SetReportCaller(args.TestMode)
	logrus.SetLevel(args.LogLevel)
	options = new(util.Options)
	options.MonEnable, options.MonСlient, err = monitoring.Init(args)
	if err != nil {
		return
	}
	options.DB = args.DB
	if options.DB == "" {
		err = errors.New("no DB server address")
		return
	}
	initParams(args)
	return
}

func initParams(args *util.Args) {
	initDBParams(args)
	initClientParams(args)
}

func initDBParams(args *util.Args) {
	db.SysNumber = len(args.Systems)
	db.KeyEx = args.KeyEx
	db.PeriodNotConfData = args.PeriodNotConfData
	db.PeriodOldData = args.PeriodOldData
}

func initClientParams(args *util.Args) {
	client.TimeoutClose = args.TimeoutClose
	client.TimeoutErrorReply = args.TimeoutErrorReply
	client.TimeoutReconnect = args.TimeoutReconnect
	client.PeriodCheckOld = args.PeriodCheckOld
}

func getSystemIds(systems []util.System) []byte {
	ids := make([]byte, len(systems))
	for i, system := range systems {
		ids[i] = system.ID
	}
	return ids
}

func initEgtsClients(options *util.Options, args *util.Args, confChan chan *db.ConfMsg) (egtsClients []client.Client, err error) {
	for _, sys := range args.Systems {
		switch sys.Protocol {
		case "EGTS":
			c := client.NewEgts(sys, options, confChan)
			egtsClients = append(egtsClients, c)
		default:
			continue
		}
	}
	return
}

func startServer(args *util.Args, options *util.Options, egtsClients []client.Client, confChan chan *db.ConfMsg) {
	listen := args.Listen
	logrus.Tracef("egts clients: %v", egtsClients)
	channels := inputChanels(egtsClients)
	logrus.Tracef("input channels: %v", channels)
	switch args.Protocol {
	case "NDTP":
		startNdtpServer(listen, options, channels, args.Systems, confChan)
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
