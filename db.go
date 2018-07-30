package main

import (
	"github.com/gomodule/redigo/redis"
)

func writeConnDB(c redis.Conn, id uint32, message []byte) error {
	_, err := c.Do("ZADD", "conn", id, message)
	if err != nil {
		//fmt.Printf("error in request write CONN_AUTH message %d: %s\n", id, err)
		return err
	}
	return nil
}

func writeToDB(Data []byte) error {
	return nil
}

func loadFirstMessage(id int) []byte {
	return []byte{}
}
