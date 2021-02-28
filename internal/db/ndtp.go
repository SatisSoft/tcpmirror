package db

import (
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
func OldPacketsNdtp(pool *Pool, sysID byte, terminalID int, offset int, logger *logrus.Entry) ([][]byte, int, error) {
	conn := pool.Get()
	defer util.CloseAndLog(conn, logger)
	notConfirmedAll := [][]byte{}
	limit := 10

	for len(notConfirmedAll) < limit {
		all, err := allNotConfirmedNdtp(conn, terminalID, logger, offset, limit)
		if err != nil {
			return nil, 0, err
		}
		lenAll := len(all)
		if lenAll == 0 {
			offset = 0
			break
		}
		if lenAll < limit {
			offset = 0
		} else {
			offset = offset + lenAll + 1
		}
		notConfirmed, err := getNotConfirmed(conn, sysID, all, logger)
		if err != nil {
			return nil, offset, err
		}
		notConfirmedAll = append(notConfirmedAll, notConfirmed...)
	}
	return notConfirmedAll, offset, nil
}

// ConfirmNdtp sets confirm bite for corresponding system to 1 and deletes confirmed packets
func ConfirmNdtp(pool *Pool, terminalID int, nphID uint32, sysID byte, logger *logrus.Entry,
	confChan chan *ConfMsg) error {
	conn := pool.Get()
	defer util.CloseAndLog(conn, logger)
	key := "ndtp:" + strconv.Itoa(int(sysID)) + ":" + strconv.Itoa(terminalID) + ":" + strconv.Itoa(int(nphID))
	res, err := redis.Bytes(conn.Do("GET", key))
	//logger.Printf("key: %v; res: %v; err: %v", key, res, err)
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
		logger.Warningln("channel is full 3")
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

func allNotConfirmedNdtp(conn redis.Conn, terminalID int, logger *logrus.Entry, offset int, limit int) ([][]byte, error) {
	max := util.Milliseconds() - PeriodNotConfData
	logger.Tracef("allNotConfirmedNdtp terminalID: %v, max: %v", terminalID, max)
	return redis.ByteSlices(conn.Do("ZRANGEBYSCORE", terminalID, 0, max, "LIMIT", offset, limit))
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
