package main

import (
	"bytes"
	"encoding/binary"
	"github.com/gomodule/redigo/redis"
	"strconv"
	"strings"
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
	_, err := c.Do("SET", key, string(mill), "ex", 50)
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
	idB := new(bytes.Buffer)
	binary.Write(idB, binary.LittleEndian, uint32(id))
	packet = append(idB.Bytes(), packet...)
	_, err := c.Do("ZADD", "rnis", time, packet)
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
	res, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", id, 0, max, "LIMIT", 0, 10))
	return res, err
}

func writeEGTSid(c redis.Conn, egtsMessageID uint16, MessageID string) (err error) {
	key := "egts:" + string(egtsMessageID)
	_, err = c.Do("SET", key, MessageID, "ex", 50)
	return
}

func deleteEGTSid(c redis.Conn, egtsMessageID uint16) (err error) {
	key := "egts:" + string(egtsMessageID)
	messageID, err := redis.Bytes(c.Do("GET", key))
	if err != nil {
	//if err != nil || resEgts == nil {
		return
	}

	messageIDSplit := strings.Split(string(messageID), ":")
	id, err := strconv.ParseUint(messageIDSplit[1], 10, 32)
	time, err := strconv.ParseInt(messageIDSplit[1], 10, 64)
	idB := new(bytes.Buffer)
	binary.Write(idB, binary.LittleEndian, id)

	packets, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", "rnis", time, time))
	if err != nil {
		return
	}
	numPackets := len(packets)
	switch {
	case numPackets > 1:
		for _, pack := range packets {
			if bytes.Compare(pack[0:4],idB.Bytes()) == 0 {
				_, err = c.Do("ZREM", "rnis", pack)
				return
			}
		}
	case numPackets == 1:
		_, err = c.Do("ZREM", "rnis", packets[0])
	}
	return
}

func removeExpiredDataEGTS(c redis.Conn) (err error) {
	max := getMill() - 259200000 //3*24*60*60*1000
	_, err = c.Do("ZREMRANGEBYSCORE", "rnis", 0, max)
	return
}

func getOldEGTS(c redis.Conn) (res [][]byte, err error) {
	max := getMill() - 60000
	res, err = redis.ByteSlices(c.Do("ZRANGEBYSCORE", "rnis", 0, max, "LIMIT", 0, 10))
	return
}
