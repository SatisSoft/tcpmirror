package main

import (
	"github.com/gomodule/redigo/redis"
)

func writeConnDB(c redis.Conn, id uint32, message []byte) error {
	key := "conn:" + string(id)
	_, err := c.Do("SET", key, message)
	return err
}

func readConnDB(c redis.Conn, id int) ([]byte, error) {
	key := "conn:" + string(id)
	res, err := redis.Bytes(c.Do("GET", key))
	return res, err
}

func writeNDTPid(c redis.Conn, id, nphID uint32, mill int64) error {
	key := "ntn:" + string(id) + ":" + string(nphID)
	_, err := c.Do("SET", key, string(mill), "ex", 120)
	return err
}

func readNDTPid(c redis.Conn, id int, nphID uint32) (int, error) {
	key := "ntn:" + string(id) + ":" + string(nphID)
	res, err := redis.Int(c.Do("GET", key))
	return res, err
}

func write2DB(c redis.Conn, data ndtpData, s *session, packet []byte, time int64) (err error) {
	err = write2NDTP(c, s.id, time, packet)
	if err != nil {
		return
	}
	if data.ToRnis.Time != 0 {
		err = write2EGTS(c, s.id, time, packet)
	}
	return
}

func write2NDTP(c redis.Conn, id int, time int64, packet []byte) error {
	_, err := c.Do("ZADD", id, time, packet)
	return err
}

func write2EGTS(c redis.Conn, id int, time int64, packet []byte) error {
	key := string(time) + ":" + string(id)
	_, err := c.Do("ZADD", "rnis", key, packet)
	return err
}

func removeFromNDTP(c redis.Conn, id int, NPHReqID uint32) error {
	time, err := readNDTPid(c, id, NPHReqID)
	if err != nil {
		return err
	}
	_, err = c.Do("ZREMRANGEBYSCORE", id, time, time)
	return err
}

func getOldNDTP(c redis.Conn, id int) ([][]byte, error) {
	max := getMill() - 60000
	res, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", id, 0, max, 0, 10))
	return res, err
}
