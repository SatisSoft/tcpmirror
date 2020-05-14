package client

import (
	"net"
	"sync"
	"time"

	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
)

const (
	defaultBufferSize = 1024
	writeTimeout      = 10 * time.Second
	readTimeout       = 180 * time.Second
)

var (
	PeriodCheckOld    int
	TimeoutClose      int
	TimeoutErrorReply int
	TimeoutReconnect  int
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
	name string
}

// Start client
func Start(client Client) {
	client.start()
}
