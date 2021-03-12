package db

import (
	"time"

	"github.com/gomodule/redigo/redis"
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
		MaxIdle:     980,
		MaxActive:   1000,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
		Wait:        true,
	}
	return &Pool{
		Pool: r,
	}
}
