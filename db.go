package main

import (
	"github.com/gomodule/redigo/redis"
)

func writeConnDB(c redis.Conn, id uint32, message []byte) error {
	key := "conn:" + string(id)
	_, err := c.Do("SET", key, message)
	return err
}

func readConnDB(c redis.Conn, id uint32) (res []byte,err error) {
	key := "conn:" + string(id)
	res, err = redis.Bytes(c.Do("GET", key))
	return
}

func

func writeToDB(Data []byte) error {
	return nil
}

