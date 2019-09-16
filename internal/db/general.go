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

const egtsKey = "egts"
const systemBytes = 4
// SysNumber is a number of clients
var SysNumber int

// Write2DB writes packet with metadata to DB
func Write2DB(pool *Pool, terminalID int, sdata []byte, logger *logrus.Entry) (err error) {
	time := util.Milliseconds()
	c := pool.Get()
	defer util.CloseAndLog(c, logger)
	//logger.Tracef("key: %v", sdata[:util.PacketStart])
	err = writeZeroConfirmation(c, uint64(time), sdata[:util.PacketStart])
	if err != nil {
		return
	}
	err = write2Ndtp(c, terminalID, time, sdata)
	if err != nil {
		return
	}
	err = write2EGTS(c, time, sdata[util.PacketStart:])
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
	_, err = c.Do("ZREMRANGEBYSCORE", egtsKey, 0, max)
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

func writeZeroConfirmation(c redis.Conn, time uint64, key []byte) error {
	val := make([]byte, 12)
	binary.LittleEndian.PutUint64(val[4:], time)
	_, err := c.Do("SET", key, val, "ex", util.Sec3Days)
	return err
}

func markSysConfirmed(conn redis.Conn, sysID byte, key []byte) error {
	_, err := conn.Do("SETBIT", key, sysID, 1)
	if err != nil{
		return err
	}
	return maybeDelete(conn, key)
}

func maybeDelete(conn redis.Conn, key []byte) error {
	n, err := conn.Do("BITCOUNT", 0, systemBytes, key)
	if err == nil && n == SysNumber {
		_, err = conn.Do("DEL", key)
	}
	return err
}

func sysNotConfirmed(conn redis.Conn, data [][]byte, sysID byte) ([][]byte, error) {
	res := [][]byte{}
	for _, id := range data {
		b, err := conn.Do("GETBIT", id, sysID)
		if err != nil {
			return nil, err
		}
		if b == 0 {
			res = append(res, id)
		}
	}
	return res, nil
}

func findPacket(conn redis.Conn, key []byte) (pack []byte, err error) {
	terminalID := util.TerminalID(key)
	time := binary.LittleEndian.Uint32(key[systemBytes:])
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


