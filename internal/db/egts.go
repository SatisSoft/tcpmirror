package db

import (
	"encoding/binary"
	"strconv"

	"log"

	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
)

// WriteEgtsID maps EgtsID to NdtpID or received EgtsID to sent EgtsID
func WriteEgtsID(conn redis.Conn, sysID byte, egtsID uint16, ID []byte) error {
	key := util.EgtsName + ":" + strconv.Itoa(int(sysID)) + ":" + strconv.Itoa(int(egtsID))
	log.Printf("WriteEgtsID key %v, ID %v\n", key, ID)
	_, err := conn.Do("SET", key, ID, "ex", KeyEx)
	return err
}

// ConfirmEgts sets confirm bite for corresponding system to 1 and deletes confirmed packets
func ConfirmEgts(conn redis.Conn, egtsID uint16, sysID byte, logger *logrus.Entry,
	confChan chan *ConfMsg) error {
	key := util.EgtsName + ":" + strconv.Itoa(int(sysID)) + ":" + strconv.Itoa(int(egtsID))
	res, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return err
	}
	data := &ConfMsg{key: res, sysID: sysID}
	select {
	case confChan <- data:
		return nil
	default:
		logger.Warningln("channel is full")
	}
	return nil
}

// OldPacketsEGTS returns not confirmed packets for corresponding system
func OldPacketsEGTS(conn redis.Conn, sysID byte, packetStart int) ([][]byte, error) {
	all, err := allNotConfirmedEGTS(conn)
	if err != nil {
		return nil, err
	}
	notConfirmedKeys, err := sysNotConfirmed(conn, all, sysID)
	if err != nil {
		return nil, err
	}
	return notConfirmed(conn, notConfirmedKeys, packetStart)
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

// RemoveExpiredEgts removes expired packet from DB
func RemoveExpiredEgts(pool *Pool, logger *logrus.Entry) (err error) {
	c := pool.Get()
	defer util.CloseAndLog(c, logger)
	max := util.Milliseconds() - util.Millisec3Days
	_, err = c.Do("ZREMRANGEBYSCORE", util.EgtsSource, 0, max)
	if err != nil {
		return
	}
	_, err = c.Do("ZREMRANGEBYSCORE", util.EgtsName, 0, max)
	return
}

// NewSessionIDEgts returns new ID of sessions between tcpmirror and data source
func NewSessionIDEgts(pool *Pool, logger *logrus.Entry) (uint64, error) {
	c := pool.Get()
	defer util.CloseAndLog(c, logger)
	key := "session:" + util.Instance
	id, err := redis.Uint64(c.Do("GET", key))
	if err != nil {
		if err == redis.ErrNil {
			id = 0
		} else {
			return 0, err
		}
	}
	_, err = c.Do("SET", key, id+1)
	return id, err
}

func allNotConfirmedEGTS(conn redis.Conn) ([][]byte, error) {
	max := util.Milliseconds() - PeriodNotConfData
	return redis.ByteSlices(conn.Do("ZRANGEBYSCORE", util.EgtsName, 0, max, "LIMIT", 0, 10000))
}

func notConfirmed(conn redis.Conn, notConfKeys [][]byte, packetStart int) ([][]byte, error) {
	res := [][]byte{}
	for _, key := range notConfKeys {
		packet, err := findPacket(conn, key, packetStart)
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

func write2Egts4Egts(c redis.Conn, time int64, sdata []byte, logger *logrus.Entry) error {
	logger.Tracef("write2Egts4Egts, time: %v; sdata: %v", time, sdata)
	res, err := c.Do("ZADD", util.EgtsSource, time, sdata)
	logger.Tracef("write2Egts4Egts time: %v; sdata: %v; res: %v; err: %v", time, sdata, res, err)
	return err
}

func findPacketEgts(conn redis.Conn, val []byte, packetStart int) ([][]byte, error) {
	time := binary.LittleEndian.Uint64(val[systemBytes:])
	packets, err := redis.ByteSlices(conn.Do("ZRANGEBYSCORE", util.EgtsSource, time, time))
	if err != nil {
		return nil, err
	}
	return packets, nil
}
