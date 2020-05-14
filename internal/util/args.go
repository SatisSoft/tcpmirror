package util

import (
	"flag"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/egorban/influx/pkg/influx"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	db                = "db_address"
	listen            = "listen_address"
	mon               = "mon_address"
	consumers         = "consumers_list"
	protocol          = "listen_protocol"
	logLevel          = "log_level"
	keyEx             = "key_ex"
	periodNotConfData = "period_notconf_data"
	periodOldData     = "period_old_data"
	periodCheckOld    = "period_check_old"
	timeoutClose      = "timeout_close"
	timeoutErrorReply = "timeout_error_reply"
	timeoutReconnect  = "timeout_reconnect"
	testMode          = "test_mode"
)

// System contains information about system which consumes data
type System struct {
	ID       byte
	Address  string
	Protocol string
	IsMaster bool
	Name     string
}

// Args contains parsed params from configuration file
type Args struct {
	// Listen address
	Listen            string
	Protocol          string
	Systems           []System
	Monitoring        string
	DB                string
	LogLevel          logrus.Level
	KeyEx             int
	PeriodNotConfData int64
	PeriodOldData     int64
	PeriodCheckOld    int
	TimeoutClose      int
	TimeoutErrorReply int
	TimeoutReconnect  int
	TestMode          bool
}

// Options contains information about DB options and monitoring options
type Options struct {
	// Is monitoring enabled
	MonEnable bool
	// Monitoring client
	MonСlient *influx.Client
	// DB sever address
	DB string
}

const egtsKey = "egts"

var (
	conf = flag.String("conf", "", "configuration file (e.g. 'config/example.toml')")
	// EgtsName is prefix for storing data in DB for different consumer systems
	EgtsName string
	Instance string
)

// ParseArgs parses configuration file
func ParseArgs() (args *Args, err error) {
	flag.Parse()
	args, err = parseConfig(*conf)
	return args, err
}

func parseConfig(conf string) (args *Args, err error) {
	args = new(Args)
	viper.SetConfigFile(conf)
	if err = viper.ReadInConfig(); err != nil {
		return
	}
	args.DB = viper.GetString(db)
	args.Listen = viper.GetString(listen)
	args.Protocol = viper.GetString(protocol)
	args.Monitoring = viper.GetString(mon)
	args.Systems = parseSystems(viper.GetStringSlice(consumers))
	args.LogLevel, err = logrus.ParseLevel(viper.GetString(logLevel))
	args.KeyEx = viper.GetInt(keyEx)
	if args.KeyEx == 0 {
		args.KeyEx = 20
	}
	args.PeriodNotConfData = viper.GetInt64(periodNotConfData)
	if args.PeriodNotConfData == 0 {
		args.PeriodNotConfData = 60000
	}
	args.PeriodOldData = viper.GetInt64(periodOldData)
	if args.PeriodOldData == 0 {
		args.PeriodOldData = 55000
	}
	args.PeriodCheckOld = viper.GetInt(periodCheckOld)
	if args.PeriodCheckOld == 0 {
		args.PeriodCheckOld = 60
	}
	args.TimeoutClose = viper.GetInt(timeoutClose)
	if args.TimeoutClose == 0 {
		args.TimeoutClose = 5
	}
	args.TimeoutErrorReply = viper.GetInt(timeoutErrorReply)
	if args.TimeoutErrorReply == 0 {
		args.TimeoutErrorReply = 5
	}
	args.TimeoutReconnect = viper.GetInt(timeoutReconnect)
	if args.TimeoutReconnect == 0 {
		args.TimeoutReconnect = 10
	}
	args.TestMode = viper.GetBool(testMode)
	Instance = strings.TrimSuffix(filepath.Base(conf), filepath.Ext(conf))
	EgtsName = egtsKey + ":" + Instance
	return
}

func parseSystems(list []string) []System {
	systems := make([]System, 0, len(list))
	for _, key := range list {
		system := parseSystem(key)
		systems = append(systems, system)
	}
	return systems
}

func parseSystem(key string) System {
	data := viper.GetStringMap(key)
	sys := System{}
	id := data["id"].(int64)
	sys.ID = byte(id)
	sys.Address = data["address"].(string)
	sys.Protocol = data["protocol"].(string)
	sys.IsMaster = data["master"].(bool)
	sys.Name = strconv.FormatInt(id, 10) + "_" + key + "_" + sys.Protocol
	return sys
}
