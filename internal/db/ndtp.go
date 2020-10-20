package db

import (
	"encoding/binary"
	"strconv"

	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
)

// WriteNDTPid maps ClientNdtpID to ServerNdtpID
func WriteNDTPid(pool *Pool, sysID byte, terminalID int, nphID uint32, packID []byte, logger *logrus.Entry) error {
	c := pool.Get()
	defer util.CloseAndLog(c, logger)
	key := "ndtp:" + strconv.Itoa(int(sysID)) + ":" + strconv.Itoa(terminalID) + ":" + strconv.Itoa(int(nphID))
	logger.Tracef("writeNdtpID key: %v", key)
	_, err := c.Do("SET", key, packID, "ex", KeyEx)
	return err
}

// WriteConnDB writes authentication packet to DB
func WriteConnDB(pool *Pool, terminalID int, logger *logrus.Entry, message []byte) error {
	c := pool.Get()
	defer util.CloseAndLog(c, logger)
	key := "conn:" + strconv.Itoa(terminalID)
	_, err := c.Do("SET", key, message)
	return err
}

// ReadConnDB reads authentication packet from DB
func ReadConnDB(pool *Pool, terminalID int, logger *logrus.Entry) ([]byte, error) {
	c := pool.Get()
	defer util.CloseAndLog(c, logger)
	key := "conn:" + strconv.Itoa(terminalID)
	res, err := redis.Bytes(c.Do("GET", key))
	logger.Tracef("ReadConnDB err: %v; key: %v; res: %v", err, key, res)
	return res, err
}

// OldPacketsNdtp returns not confirmed packets for corresponding system
func OldPacketsNdtp(pool *Pool, sysID byte, terminalID int, logger *logrus.Entry) ([][]byte, error) {
	conn := pool.Get()
	defer util.CloseAndLog(conn, logger)
	all, err := allNotConfirmedNdtp(conn, terminalID, logger)
	logger.Tracef("allNotConfirmed: %v, %v", err, all)
	if err != nil {
		return nil, err
	}
	return getNotConfirmed(conn, sysID, all, logger)
}

// ConfirmNdtp sets confirm bite for corresponding system to 1 and deletes confirmed packets
func ConfirmNdtp(pool *Pool, terminalID int, nphID uint32, sysID byte, logger *logrus.Entry,
	confChan chan *ConfMsg) error {
	conn := pool.Get()
	defer util.CloseAndLog(conn, logger)
	key := "ndtp:" + strconv.Itoa(int(sysID)) + ":" + strconv.Itoa(terminalID) + ":" + strconv.Itoa(int(nphID))
	res, err := redis.Bytes(conn.Do("GET", key))
	logger.Printf("key: %v; res: %v; err: %v", key, res, err)
	if err != nil {
		return err
	}
	data := &ConfMsg{key: res, sysID: sysID}
	logger.Tracef("Send to confChan: %v", data)
	logger.Tracef("deleteManChan: %v", confChan)
	select {
	case confChan <- data:
		return nil
	default:
		logger.Warningln("channel is full")
	}
	return nil
}

// SetNph writes Nph ID to db
func SetNph(pool *Pool, sysID byte, terminalID int, nphID uint32, logger *logrus.Entry) error {
	conn := pool.Get()
	defer util.CloseAndLog(conn, logger)
	key := "max:" + strconv.Itoa(int(sysID)) + ":" + strconv.Itoa(terminalID)
	res, err := conn.Do("SET", key, nphID)
	logger.Tracef("SetNph key: %v, r: %v, nphID: %v; err: %v", key, res, nphID, err)
	return err
}

// GetNph gets Nph ID from db
func GetNph(pool *Pool, sysID byte, terminalID int, logger *logrus.Entry) (uint32, error) {
	conn := pool.Get()
	defer util.CloseAndLog(conn, logger)
	key := "max:" + strconv.Itoa(int(sysID)) + ":" + strconv.Itoa(terminalID)
	nphID, err := redis.Int(conn.Do("GET", key))
	logger.Tracef("GetNph key: %v, nphID: %d, err: %v", key, nphID, err)
	if err == redis.ErrNil {
		return 0, nil
	}
	return uint32(nphID), err
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

func write2Ndtp(c redis.Conn, terminalID int, time int64, sdata []byte, logger *logrus.Entry) error {
	logger.Tracef("write2Ndtp terminalID: %v, time: %v; sdata: %v", terminalID, time, sdata)
	res, err := c.Do("ZADD", terminalID, time, sdata)
	logger.Tracef("write2Ndtp terminalID: %v, time: %v; sdata: %v; res: %v; err: %v", terminalID, time, sdata, res, err)
	return err
}

func allNotConfirmedNdtp(conn redis.Conn, terminalID int, logger *logrus.Entry) ([][]byte, error) {
	max := util.Milliseconds() - PeriodNotConfData
	logger.Tracef("allNotConfirmedNdtp terminalID: %v, max: %v", terminalID, max)
	return redis.ByteSlices(conn.Do("ZRANGEBYSCORE", terminalID, 0, max, "LIMIT", 0, 60000))
}

func getNotConfirmed(conn redis.Conn, sysID byte, packets [][]byte, logger *logrus.Entry) ([][]byte, error) {
	res := make([][]byte, 0)
	for _, packet := range packets {
		id := packet[:util.PacketStart]
		isConf, err := isConfirmed(conn, id, sysID)
		if err != nil {
			logger.Tracef("getNotConfirmed 1: sysID: %v, err: %v", sysID, res)
			return nil, err
		}
		if !isConf {
			res = append(res, packet)
		}
	}
	logger.Tracef("getNotConfirmed 2: sysID: %v, res: %v", sysID, res)
	return res, nil
}

func findPacketNdtp(conn redis.Conn, key []byte, val []byte, packetStart int) ([][]byte, error) {
	terminalID := util.TerminalID(key)
	time := binary.LittleEndian.Uint64(val[systemBytes:])
	packets, err := redis.ByteSlices(conn.Do("ZRANGEBYSCORE", terminalID, time, time))
	if err != nil {
		return nil, err
	}
	return packets, nil
}
