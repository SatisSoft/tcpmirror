package db

import (
	"github.com/gomodule/redigo/redis"
	"time"
)

// Pool defines pool of connections to DB
type Pool struct {
	*redis.Pool
}

// NewPool create new pool of connections to DB
func NewPool(dbAddress string) (pool *Pool) {
	return newPool(dbAddress)
}

// Close closes pull of connections to DB
func (pool Pool) Close() error {
	return pool.Pool.Close()
}

func newPool(addr string) *Pool {
	r := &redis.Pool{
		MaxIdle:     20,
		MaxActive:   0,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
	return &Pool{
		Pool: r,
	}
}
