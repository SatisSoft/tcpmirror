package db

import (
	"strconv"

	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
)

// WriteEgtsID maps EgtsID to NdtpID or received EgtsID to sent EgtsID
func WriteEgtsID(conn redis.Conn, sysID byte, egtsID uint16, ID []byte) error {
	key := util.EgtsName + ":" + strconv.Itoa(int(sysID)) + ":" + strconv.Itoa(int(egtsID))
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
		logger.Warningln("channel is full 2")
	}
	return nil
}

// OldPacketsEGTS returns not confirmed packets for corresponding system
func OldPacketsEGTS(conn redis.Conn, sysID byte, offset int) ([][]byte, int, error) {
	allNotConfirmed := [][]byte{}
	limit := 100 * 60 //100p/sec
	lenNotConf := 0

	for limit > 0 && lenNotConf < limit {
		all, err := allNotConfirmedEGTS(conn, offset, limit)
		if err != nil {
			return nil, offset, err
		}
		notConfirmedKeys, err := sysNotConfirmed(conn, all, sysID)
		if err != nil {
			return nil, offset, err
		}

		notConfirmed, err := notConfirmed(conn, notConfirmedKeys)
		if err != nil {
			return nil, offset, err
		}

		lenAll := len(all)
		lenNotConf0 := len(notConfirmed)

		if lenNotConf != 0 {
			allNotConfirmed = append(allNotConfirmed, notConfirmed...)
			lenNotConf = lenNotConf + lenNotConf0
		}

		if lenAll < limit {
			offset = 0
			break
		} else {
			offset = offset + lenAll + 1
		}

		if lenNotConf < limit {
			limit = limit - lenNotConf
		}
	}

	return allNotConfirmed, offset, nil
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

func allNotConfirmedEGTS(conn redis.Conn, offset int, limit int) ([][]byte, error) {
	max := util.Milliseconds() - PeriodNotConfData
	return redis.ByteSlices(conn.Do("ZRANGEBYSCORE", util.EgtsName, 0, max, "LIMIT", offset, limit))
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
