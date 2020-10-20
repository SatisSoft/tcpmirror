package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
)

const systemBytes = 4

var (
	// SysNumber is a number of clients
	SysNumber         int
	KeyEx             int
	PeriodNotConfData int64
	PeriodOldData     int64
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

// Write2DB4Egts writes packet with metadata to DB
func Write2DB4Egts(pool *Pool, sdata []byte, logger *logrus.Entry) (err error) {
	logger.Tracef("Write2DB4Egts, sdata: %v", sdata)
	time := util.Milliseconds()
	c := pool.Get()
	defer util.CloseAndLog(c, logger)
	logger.Tracef("writeZeroConfirmation time: %v; key: %v", time, sdata[:util.PacketStartEgts])
	err = writeZeroConfirmation(c, uint64(time), sdata[:util.PacketStartEgts])
	if err != nil {
		return
	}
	err = write2Egts4Egts(c, time, sdata, logger)
	if err != nil {
		return
	}
	err = write2EGTS(c, time, sdata[:util.PacketStartEgts])
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

// IsOldData checks if message is old and should not be sending again
func IsOldData(pool *Pool, meta []byte, logger *logrus.Entry) bool {
	c := pool.Get()
	defer util.CloseAndLog(c, logger)
	return CheckOldData(c, meta, logger)
}

// CheckOldData checks if message is old and should not be sending again
func CheckOldData(conn redis.Conn, meta []byte, logger *logrus.Entry) bool {
	val, err := redis.Bytes(conn.Do("GET", meta))
	logger.Tracef("isOldData err: %v; key: %v; val: %v", err, meta, val)
	if err == redis.ErrNil {
		logger.Tracef("isOldData detected empty result: %v;", val)
		return true
	}
	if len(val) < systemBytes {
		_ = fmt.Errorf("got short result: %v", val)
		return true
	}
	time := binary.LittleEndian.Uint64(val[systemBytes:])
	min := uint64(util.Milliseconds() - PeriodOldData)
	logger.Tracef("isOldData key: %v; time: %d; now: %d", meta, time, min)
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

func findPacket(conn redis.Conn, key []byte, packetStart int) (pack []byte, err error) {
	val, err := redis.Bytes(conn.Do("GET", key))
	logrus.Tracef("findPack key = %v, val = %v, err = %v", key, val, err)
	if err != nil {
		return
	}
	if len(val) < systemBytes {
		err = fmt.Errorf("got short result: %v", val)
		return
	}
	var packets [][]byte
	if util.EgtsSource != "" {
		packets, err = findPacketEgts(conn, val, packetStart)
	} else {
		packets, err = findPacketNdtp(conn, key, val, packetStart)
	}
	if err != nil {
		return nil, err
	}
	numPackets := len(packets)
	switch {
	case numPackets > 1:
		for _, p := range packets {
			if bytes.Compare(p[:packetStart], key) == 0 {
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
