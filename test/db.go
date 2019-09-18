package test

import (
	"github.com/ashirko/tcpmirror/internal/db"
	"github.com/gomodule/redigo/redis"
)

func clearDB(conn db.Conn) error {
	_, err := conn.Do("FLUSHALL")
	return err
}

func getAllKeys(conn db.Conn) ([][]byte, error) {
	return redis.ByteSlices(conn.Do("KEYS", "*"))
}
