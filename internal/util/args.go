package util

import (
	"flag"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"path/filepath"
	"strings"
)

const (
	db        = "db_address"
	listen    = "listen_address"
	mon       = "mon_address"
	consumers = "consumers_list"
	protocol  = "listen_protocol"
	logLevel  = "log_level"
)

type System struct {
	ID       byte
	Address  string
	Protocol string
	IsMaster bool
}

type Args struct {
	Listen     string
	Protocol   string
	Systems    []System
	Monitoring string
	DB         string
	LogLevel   logrus.Level
}

type Options struct {
	// Is monitoring enabled
	Mon bool
	// DB sever address
	DB string
}

const egtsKey = "egts"

var (
	conf = flag.String("conf", "", "configuration file (e.g. 'config/example.toml')")
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
