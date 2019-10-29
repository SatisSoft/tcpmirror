package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"strconv"
)

const systemBytes = 4

// SysNumber is a number of clients
var (
    SysNumber int
    KeyEx               int
    PeriodNotConfData   int64
    PeriodOldData       int64
)

// Write2DB writes packet with metadata to DB
func Write2DB(pool *Pool, terminalID int, sdata []byte, logger *logrus.Entry) (err error) {
	logger.Tracef("Write2DB terminalID: %d, sdata: %v", terminalID, sdata)
	time := util.Milliseconds()
	c := pool.Get()
	defer util.CloseAndLog(c, logger)
	logger.Tracef("writeZeroConfirmation time: %v; key: %v", time, sdata[:util.PacketStart])
	err = writeZeroConfirmation(c, uint64(time), sdata[:util.PacketStart])
	if err != nil {
		return
	}
	err = write2Ndtp(c, terminalID, time, sdata, logger)
	if err != nil {
		return
	}
	err = write2EGTS(c, time, sdata[:util.PacketStart])
	return
}

// RemoveExpired removes expired packet from DB
func RemoveExpired(pool *Pool, terminalID int, logger *logrus.Entry) (err error) {
	c := pool.Get()
	defer util.CloseAndLog(c, logger)
	max := util.Milliseconds() - util.Millisec3Days
	_, err = c.Do("ZREMRANGEBYSCORE", terminalID, 0, max)
	if err != nil {
		return
	}
	_, err = c.Do("ZREMRANGEBYSCORE", util.EgtsName, 0, max)
	return
}

// NewSessionID returns new ID of sessions between tcpmirror and terminal
func NewSessionID(pool *Pool, terminalID int, logger *logrus.Entry) (int, error) {
	c := pool.Get()
	defer util.CloseAndLog(c, logger)
	key := "session:" + strconv.Itoa(terminalID)
	id, err := redis.Int(c.Do("GET", key))
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

func IsOldData(pool *Pool, message []byte, logger *logrus.Entry) bool {
	c := pool.Get()
	defer util.CloseAndLog(c, logger)
	return CheckOldData(c, message, logger)
}

func CheckOldData(conn redis.Conn, message []byte, logger *logrus.Entry) bool {
	val, err := redis.Bytes(conn.Do("GET", message[:util.PacketStart]))
	logger.Tracef("isOldData err: %v; key: %v; val: %v", err, message[:util.PacketStart], val)
	if err == redis.ErrNil {
		logger.Tracef("isOldData detected empty result: %v;", val)
		return true
	}
	time := binary.LittleEndian.Uint64(val[systemBytes:])
	min := uint64(util.Milliseconds() - PeriodOldData)
	logger.Tracef("isOldData key: %v; time: %d; now: %d", message[:util.PacketStart], time, min)
	if time < min {
		logger.Tracef("isOldData detected old time: %d, val: %v", time, val)
		return true
	}
	return false
}

func writeZeroConfirmation(c redis.Conn, time uint64, key []byte) error {
	val := make([]byte, 12)
	binary.LittleEndian.PutUint64(val[4:], time)
	_, err := c.Do("SET", key, val, "ex", util.Sec3Days)
	return err
}
func markSysConfirmed(conn redis.Conn, sysID byte, key []byte) error {
	logrus.Tracef("markSysConfirmed sysID %d, key %v", sysID, key)
	_, err := conn.Do("SETBIT", key, sysID, 1)
	if err != nil {
		return err
	}
	return maybeDelete(conn, key)
}

func maybeDelete(conn redis.Conn, key []byte) error {
	n, err := redis.Int(conn.Do("BITCOUNT", key, 0, systemBytes-1))
	logrus.Tracef("maybeDelete n = %d, key %v", n, key)
	if err == nil && n == SysNumber {
		err = deletePacket(conn, key)
	}
	return err
}

func deletePacket(conn redis.Conn, key []byte) error {
	packet, err := findPacket(conn, key)
	logrus.Tracef("deletePacket key = %v, packet = %v, err = %v", key, packet, err)
	if err != nil {
		return err
	}
	terminalID := util.TerminalID(key)
	res, err := conn.Do("ZREM", util.EgtsName, key)
	logrus.Tracef("del 1 res = %v, err = %v", res, err)
	if err != nil {
		return err
	}
	res, err = conn.Do("ZREM", terminalID, packet)
	logrus.Tracef("del 2 res = %v, err = %v", res, err)
	if err != nil {
		return err
	}
	res, err = conn.Do("DEL", key)
	logrus.Tracef("del 3 res = %v, err = %v", res, err)
	if err != nil {
		return err
	}
	return err
}

func sysNotConfirmed(conn redis.Conn, data [][]byte, sysID byte) ([][]byte, error) {
	res := [][]byte{}
	for _, id := range data {
		isConf, err := isConfirmed(conn, id, sysID)
		if err != nil {
			return nil, err
		}
		if !isConf {
			res = append(res, id)
		}
	}
	return res, nil
}

func isConfirmed(conn redis.Conn, id []byte, sysID byte) (isConf bool, err error) {
	ex, err := redis.Int(conn.Do("EXISTS", id))
	logrus.Tracef("isConfirmed ex: %v; err: %v", ex, err)
	if ex == 0 {
		return true, err
	}
	b, err := redis.Int(conn.Do("GETBIT", id, sysID))
	logrus.Tracef("isConfirmed b: %v; err: %v; sysID: %d; id %v;", b, err, sysID, id)
	if b == 1 {
		isConf = true
	}
	return
}

func findPacket(conn redis.Conn, key []byte) (pack []byte, err error) {
	//val0, err := redis.Bytes(conn.Do("GET", key))
	val0, err := conn.Do("GET", key)
	val1, err := redis.Bytes(val0, err)
	logrus.Tracef("findPack key = %v, val0 = %v, val1 = %v, err = %v", key, val0, val1, err)
	if err != nil {
		return
	}
	terminalID := util.TerminalID(key)
	time := binary.LittleEndian.Uint64(val1[systemBytes:])
	packets, err := redis.ByteSlices(conn.Do("ZRANGEBYSCORE", terminalID, time, time))
	if err != nil {
		return nil, err
	}
	numPackets := len(packets)
	switch {
	case numPackets > 1:
		for _, p := range packets {
			if bytes.Compare(p[:util.PacketStart], key) == 0 {
				return p, nil
			}
		}
		err = fmt.Errorf("packet not found")
	case numPackets == 1:
		pack = packets[0]
	default:
		err = fmt.Errorf("packet not found")
	}
	return pack, err
}
