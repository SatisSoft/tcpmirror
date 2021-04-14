package client

import (
	"net"
	"sync"
	"time"

	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
)

const (
	defaultBufferSize = 65536
	writeTimeout      = 10 * time.Second
	readTimeout       = 180 * time.Second
	recsAtPacketEgts  = 3
	numPcktsToSend    = 10
	realTimeTypeMon   = "realtime"
	oldTimeTypeMon    = "old"
	replyTypeMon      = "reply"
	authTypeMon       = "auth"
	controlTypeMon    = "control"
	undefTypeMon      = "undefined"
)

var (
	TimeoutCloseSec      int
	TimeoutErrorReplySec int
	TimeoutReconnectSec  int

	PeriodCheckOldEgtsMs     int
	BatchOldEgts             int
	PeriodSendBatchOldEgtsMs int
	PeriodSendOldEgtsMs      int
	WaitConfEgtsMs           int

	MaxOldToSendNdtp        int
	PeriodCheckOldNdtpMs    int
	PeriodSendOnlyOldNdtpMs int
	WaitConfNdtpMs          int
)

// Client describes general client
type Client interface {
	InputChannel() chan []byte
	OutputChannel() chan []byte
	start()
}

type connection struct {
	conn         net.Conn
	open         bool
	reconnecting bool
	muRecon      sync.Mutex
}

type info struct {
	id      byte
	address string
	logger  *logrus.Entry
	*util.Options
}

// Start client
func Start(client Client) {
	client.start()
}