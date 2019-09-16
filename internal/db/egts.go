package db

import (
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/gomodule/redigo/redis"
	"strconv"
)

// WriteEgtsID maps EgtsID to NdtpID
func WriteEgtsID(conn redis.Conn, egtsID uint16, packetID []byte) error {
	key := "egts:" + strconv.Itoa(int(egtsID))
	_, err := conn.Do("SET", key, packetID, "ex", 20)
	return err
}

// ConfirmEgts sets confirm bite for corresponding system to 1 and deletes confirmed packets
func ConfirmEgts(conn redis.Conn, egtsID uint16, sysID byte) error {
	key := "egts:" + strconv.Itoa(int(egtsID))
	res, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return err
	}
	return markSysConfirmed(conn, sysID, res)
}

// OldPacketsEGTS returns not confirmed packets for corresponding system
func OldPacketsEGTS(conn redis.Conn, sysID byte) ([][]byte, error) {
	all, err := allNotConfirmedEGTS(conn)
	if err != nil {
		return nil, err
	}
	notConfirmedKeys, err := sysNotConfirmed(conn, all, sysID)
	if err != nil {
		return nil, err
	}
	return notConfirmed(conn, notConfirmedKeys)
}

func allNotConfirmedEGTS(conn redis.Conn) ([][]byte, error) {
	max := util.Milliseconds() - 60000
	return redis.ByteSlices(conn.Do("ZRANGEBYSCORE", egtsKey, 0, max, "LIMIT", 0, 10000))
}

func notConfirmed(conn redis.Conn, notConfKeys [][]byte) ([][]byte, error) {
	res := [][]byte{}
	for _, key := range notConfKeys {
		packet, err := findPacket(conn, key)
		if err != nil {
			return nil, err
		}
		res = append(res, packet)
	}
	return res, nil
}

func write2EGTS(c redis.Conn, time int64, key []byte) error {
	_, err := c.Do("ZADD", egtsKey, time, key)
	return err
}
