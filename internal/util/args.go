package util

import (
	"flag"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"path/filepath"
	"strings"
)

const (
	db                  = "db_address"
	listen              = "listen_address"
	mon                 = "mon_address"
	consumers           = "consumers_list"
	protocol            = "listen_protocol"
	logLevel            = "log_level"
	keyEx               = "key_ex"
	periodNotConfData   = "period_notconf_data"
	periodOldData       = "period_old_data"
	periodCheckOld      = "period_check_old"
	timeoutClose        = "timeout_close"
	timeoutErrorReply   = "timeout_error_reply"
    timeoutReconnect    = "timeout_reconnect"
)

type System struct {
	ID       byte
	Address  string
	Protocol string
	IsMaster bool
}

type Args struct {
	Listen              string
	Protocol            string
	Systems             []System
	Monitoring          string
	DB                  string
	LogLevel            logrus.Level
	KeyEx               int
	PeriodNotConfData   int64
	PeriodOldData       int64
	PeriodCheckOld      int
	TimeoutClose        int
	TimeoutErrorReply   int
    TimeoutReconnect    int
}

type Options struct {
	// Is monitoring enabled
	Mon                 bool
	// DB sever address
	DB                  string
	// Expiration time keys
	KeyEx               int
	// Period for not confirmed data
	PeriodNotConfData   int64
	// Period for old data
	PeriodOldData       int64
	// Period check old data
	PeriodCheckOld      int
    // Timeout if connection is not open
    TimeoutClose        int
    // Timeout if error is occurred during waiting reply
    TimeoutErrorReply   int
    // Timeout for attemps reconnect
    TimeoutReconnect    int
}

const egtsKey = "egts"

var (
	conf     = flag.String("conf", "", "configuration file (e.g. 'config/example.toml')")
	EgtsName string
)

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
	args.PeriodNotConfData = viper.GetInt64(periodNotConfData)
	args.PeriodOldData = viper.GetInt64(periodOldData)
	args.PeriodCheckOld = viper.GetInt(periodCheckOld)
	args.TimeoutClose = viper.GetInt(timeoutClose)
	args.TimeoutErrorReply = viper.GetInt(timeoutErrorReply)
	args.TimeoutReconnect = viper.GetInt(timeoutReconnect)
	EgtsName = egtsKey + ":" + strings.TrimSuffix(filepath.Base(conf), filepath.Ext(conf))
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
	sys.ID = byte(data["id"].(int64))
	sys.Address = data["address"].(string)
	sys.Protocol = data["protocol"].(string)
	sys.IsMaster = data["master"].(bool)
	return sys
}
