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
	listen    = "listen_address"
	protocol  = "listen_protocol"
	db        = "db_address"
	mon       = "mon_address"
	logLevel  = "log_level"
	testMode  = "test_mode"
	consumers = "consumers_list"

	keyEx                = "key_ex"
	timeoutCloseSec      = "timeout_close_sec"
	timeoutErrorReplySec = "timeout_error_reply_sec"
	timeoutReconnectSec  = "timeout_reconnect_sec"
	redisMaxIdle         = "redis_max_idle"
	redisMaxActive       = "redis_max_active"
	periodOldDataMs      = "period_old_data_ms"

	periodNotConfDataEgtsMs  = "period_notconf_data_egts_ms"
	periodCheckOldEgtsMs     = "period_check_old_egts_ms"
	batchOldEgts             = "batch_old_egts"
	periodSendBatchOldEgtsMs = "period_send_batch_old_egts_ms"
	waitConfEgtsMs           = "wait_conf_egts_ms"

	periodNotConfDataNdtpMs = "period_notconf_data_ndtp_ms"
	periodCheckOldNdtpMs    = "period_check_old_ndtp_ms"
	periodSendOnlyOldNdtpMs = "period_send_only_old_ndtp_ms"
	waitConfNdtpMs          = "wait_conf_ndtp_ms"
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
	Listen     string
	Protocol   string
	DB         string
	Monitoring string
	LogLevel   logrus.Level
	TestMode   bool
	Systems    []System

	KeyEx                int
	TimeoutCloseSec      int
	TimeoutErrorReplySec int
	TimeoutReconnectSec  int
	RedisMaxIdle         int
	RedisMaxActive       int
	PeriodOldDataMs      int64

	PeriodNotConfDataEgtsMs  int64
	PeriodCheckOldEgtsMs     int
	BatchOldEgts             int
	PeriodSendBatchOldEgtsMs int
	WaitConfEgtsMs           int

	PeriodNotConfDataNdtpMs int64
	PeriodCheckOldNdtpMs    int
	PeriodSendOnlyOldNdtpMs int
	WaitConfNdtpMs          int
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
	args.Listen = viper.GetString(listen)
	args.Protocol = viper.GetString(protocol)
	args.DB = viper.GetString(db)
	args.Monitoring = viper.GetString(mon)
	args.LogLevel, err = logrus.ParseLevel(viper.GetString(logLevel))
	args.TestMode = viper.GetBool(testMode)
	args.Systems = parseSystems(viper.GetStringSlice(consumers))

	// general args
	args.KeyEx = viper.GetInt(keyEx)
	if args.KeyEx == 0 {
		args.KeyEx = 20
	}
	args.TimeoutCloseSec = viper.GetInt(timeoutCloseSec)
	if args.TimeoutCloseSec == 0 {
		args.TimeoutCloseSec = 5
	}
	args.TimeoutErrorReplySec = viper.GetInt(timeoutErrorReplySec)
	if args.TimeoutErrorReplySec == 0 {
		args.TimeoutErrorReplySec = 5
	}
	args.TimeoutReconnectSec = viper.GetInt(timeoutReconnectSec)
	if args.TimeoutReconnectSec == 0 {
		args.TimeoutReconnectSec = 10
	}
	args.RedisMaxIdle = viper.GetInt(redisMaxIdle)
	if args.RedisMaxIdle == 0 {
		args.RedisMaxIdle = 220
	}
	args.RedisMaxActive = viper.GetInt(redisMaxActive)
	if args.RedisMaxActive == 0 {
		args.RedisMaxActive = 220
	}
	args.PeriodOldDataMs = viper.GetInt64(periodOldDataMs)
	if args.PeriodOldDataMs == 0 {
		args.PeriodOldDataMs = 1000 //55000
	}

	// egts args
	args.PeriodNotConfDataEgtsMs = viper.GetInt64(periodNotConfDataEgtsMs)
	if args.PeriodNotConfDataEgtsMs == 0 {
		args.PeriodNotConfDataEgtsMs = 2000
	}
	args.PeriodCheckOldEgtsMs = viper.GetInt(periodCheckOldEgtsMs)
	if args.PeriodCheckOldEgtsMs == 0 {
		args.PeriodCheckOldEgtsMs = 30000
	}
	args.BatchOldEgts = viper.GetInt(batchOldEgts)
	if args.BatchOldEgts == 0 {
		args.BatchOldEgts = 350
	}
	args.PeriodSendBatchOldEgtsMs = viper.GetInt(periodSendBatchOldEgtsMs)
	if args.PeriodSendBatchOldEgtsMs == 0 {
		args.PeriodSendBatchOldEgtsMs = 100
	}
	args.WaitConfEgtsMs = viper.GetInt(waitConfEgtsMs)
	if args.WaitConfEgtsMs == 0 {
		args.WaitConfEgtsMs = 60000
	}

	// ndtp args
	args.PeriodNotConfDataNdtpMs = viper.GetInt64(periodNotConfDataNdtpMs)
	if args.PeriodNotConfDataNdtpMs == 0 {
		args.PeriodNotConfDataNdtpMs = 2000
	}
	args.PeriodCheckOldNdtpMs = viper.GetInt(periodCheckOldNdtpMs)
	if args.PeriodCheckOldNdtpMs == 0 {
		args.PeriodCheckOldNdtpMs = 30000
	}
	args.PeriodSendOnlyOldNdtpMs = viper.GetInt(periodSendOnlyOldNdtpMs)
	if args.PeriodSendOnlyOldNdtpMs == 0 {
		args.PeriodSendOnlyOldNdtpMs = 30000
	}
	args.WaitConfNdtpMs = viper.GetInt(waitConfNdtpMs)
	if args.WaitConfNdtpMs == 0 {
		args.WaitConfNdtpMs = 60000
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
