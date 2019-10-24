package db

import (
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/gomodule/redigo/redis"
	"strconv"
)

// WriteEgtsID maps EgtsID to NdtpID
func WriteEgtsID(conn redis.Conn, sysID byte, egtsID uint16, packetID []byte, expire int) error {
	key := util.EgtsName + ":" + strconv.Itoa(int(sysID)) + ":" + strconv.Itoa(int(egtsID))
	_, err := conn.Do("SET", key, packetID, "ex", expire)
	return err
}

// ConfirmEgts sets confirm bite for corresponding system to 1 and deletes confirmed packets
func ConfirmEgts(conn redis.Conn, egtsID uint16, sysID byte) error {
	key := util.EgtsName + ":" + strconv.Itoa(int(sysID)) + ":" + strconv.Itoa(int(egtsID))
	res, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return err
	}
	return markSysConfirmed(conn, sysID, res)
}

// OldPacketsEGTS returns not confirmed packets for corresponding system
func OldPacketsEGTS(conn redis.Conn, sysID byte, period int64) ([][]byte, error) {
	all, err := allNotConfirmedEGTS(conn, period)
	if err != nil {
		return nil, err
	}
	notConfirmedKeys, err := sysNotConfirmed(conn, all, sysID)
	if err != nil {
		return nil, err
	}
	return notConfirmed(conn, notConfirmedKeys)
}

// SetEgtsID writes Egts IDs to db
func SetEgtsID(conn redis.Conn, sysID byte, reqID uint16) error {
	key := "max:" + strconv.Itoa(int(sysID))
	_, err := conn.Do("SET", key, reqID)
	return err
}

// GetEgtsID gets Egts IDs from db
func GetEgtsID(conn redis.Conn, sysID byte) (req uint16, err error) {
	key := "max:" + strconv.Itoa(int(sysID))
	res, err := redis.Int(conn.Do("GET", key))
	if err != nil && err != redis.ErrNil {
		return
	}
	req = uint16(res)
	return req, nil
}

func allNotConfirmedEGTS(conn redis.Conn, period int64) ([][]byte, error) {
	max := util.Milliseconds() - period
	return redis.ByteSlices(conn.Do("ZRANGEBYSCORE", util.EgtsName, 0, max, "LIMIT", 0, 10000))
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
	_, err := c.Do("ZADD", util.EgtsName, time, key)
	return err
}
