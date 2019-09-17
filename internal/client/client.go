package client

import (
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
	"time"
)

const (
	defaultBufferSize = 1024
	writeTimeout      = 10 * time.Second
	readTimeout       = 180 * time.Second
)

// Client describes general client
type Client interface {
	InputChannel() chan []byte
	OutputChannel() chan []byte
	start()
	stop() error
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

// Stop client
func Stop(client Client) error {
	return client.stop()
}