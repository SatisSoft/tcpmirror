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
	db                    = "db_address"
	listen                = "listen_address"
	mon                   = "mon_address"
	consumers             = "consumers_list"
	protocol              = "listen_protocol"
	logLevel              = "log_level"
	keyEx                 = "key_ex"
	periodNotConfDataEgts = "period_notconf_data_egts"
	periodNotConfDataNdtp = "period_notconf_data_ndtp"
	periodOldData         = "period_old_data"
	periodCheckOldEgts    = "period_check_old_egts"
	periodCheckOldNdtp    = "period_check_old_ndtp"
	timeoutClose          = "timeout_close"
	timeoutErrorReply     = "timeout_error_reply"
	timeoutReconnect      = "timeout_reconnect"
	testMode              = "test_mode"
	maxToSendOldEgts      = "max_to_send_old_egts"
	limitOldEgts          = "limit_old_egts"
	maxToSendOldNdtp      = "max_to_send_old_ndtp"
	limitOldNdtp          = "limit_old_ndtp"
	redisMaxIdle          = "redis_max_idle"
	redisMaxActive        = "redis_max_active"
	periodSendOldNdtp     = "period_send_old_ndtp"
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
	Listen                string
	Protocol              string
	Systems               []System
	Monitoring            string
	DB                    string
	LogLevel              logrus.Level
	KeyEx                 int
	PeriodNotConfDataEgts int64
	PeriodNotConfDataNdtp int64
	PeriodOldData         int64
	PeriodCheckOldEgts    int
	PeriodCheckOldNdtp    int
	TimeoutClose          int
	TimeoutErrorReply     int
	TimeoutReconnect      int
	TestMode              bool
	MaxToSendOldEgts      int
	LimitOldEgts          int
	MaxToSendOldNdtp      int
	LimitOldNdtp          int
	RedisMaxIdle          int
	RedisMaxActive        int
	PeriodSendOldNdtp     int
}

// Options contains information about DB options and monitoring options
type Options struct {
	// Is monitoring enabled
	MonEnable bool
	// Monitoring client
	MonСlient *influx.Client
	// DB sever address
	DB string
	// Server protocol
	ServerProtocol string
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
	args.PeriodNotConfDataEgts = viper.GetInt64(periodNotConfDataEgts)
	if args.PeriodNotConfDataEgts == 0 {
		args.PeriodNotConfDataEgts = 60000
	}
	args.PeriodNotConfDataNdtp = viper.GetInt64(periodNotConfDataNdtp)
	if args.PeriodNotConfDataNdtp == 0 {
		args.PeriodNotConfDataNdtp = 60000
	}
	args.PeriodOldData = viper.GetInt64(periodOldData)
	if args.PeriodOldData == 0 {
		args.PeriodOldData = 1000 //55000
	}
	args.PeriodCheckOldEgts = viper.GetInt(periodCheckOldEgts)
	if args.PeriodCheckOldEgts == 0 {
		args.PeriodCheckOldEgts = 60
	}
	args.PeriodCheckOldNdtp = viper.GetInt(periodCheckOldNdtp)
	if args.PeriodCheckOldNdtp == 0 {
		args.PeriodCheckOldNdtp = 60
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
	args.MaxToSendOldEgts = viper.GetInt(maxToSendOldEgts)
	if args.MaxToSendOldEgts == 0 {
		args.MaxToSendOldEgts = 600000
	}
	args.LimitOldEgts = viper.GetInt(limitOldEgts)
	if args.LimitOldEgts == 0 {
		args.LimitOldEgts = 600000
	}
	args.MaxToSendOldNdtp = viper.GetInt(maxToSendOldNdtp)
	if args.MaxToSendOldNdtp == 0 {
		args.MaxToSendOldNdtp = 600
	}
	args.LimitOldNdtp = viper.GetInt(limitOldNdtp)
	if args.LimitOldNdtp == 0 {
		args.LimitOldNdtp = 600
	}
	args.RedisMaxIdle = viper.GetInt(redisMaxIdle)
	if args.RedisMaxIdle == 0 {
		args.RedisMaxIdle = 400
	}
	args.RedisMaxActive = viper.GetInt(redisMaxActive)
	if args.RedisMaxActive == 0 {
		args.RedisMaxActive = 400
	}
	args.PeriodSendOldNdtp = viper.GetInt(periodSendOldNdtp)
	if args.PeriodSendOldNdtp == 0 {
		args.PeriodSendOldNdtp = 30
	}

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
