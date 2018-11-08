package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

//from server:
//conn:id - NPH_SGC_CONN_REQUEST
//gc_s:id1:id2 - NPH_SRV_GENERIC_CONTROLS
//ext_s:id - NPH_SRV_EXTERNAL_DEVICE

//from client:
//ext_c:id; ext_id_c:id1:id2 - NPH_SRV_EXTERNAL_DEVICE
//id, nid:id1:id2 - NPH_SRV_NAVDATA for NDTP Server
//rnis - NPH_SRV_NAVDATA for EGTS Server

func writeConnDB(c redis.Conn, id uint32, message []byte, logger *logrus.Entry) error {
	key := "conn:" + strconv.Itoa(int(id))
	logger.Tracef("key: %s; message: %v", key, message)
	_, err := c.Do("SET", key, message)
	logger.Tracef("err: %v", err)
	return err
}

func readConnDB(c redis.Conn, s *session) ([]byte, error) {
	key := "conn:" + strconv.Itoa(s.id)
	res, err := redis.Bytes(c.Do("GET", key))
	s.logger.Tracef("key: %s; message: %v", key, res)
	s.logger.Tracef("err: %v", err)
	return res, err
}

func writeNDTPid(c redis.Conn, s *session, nphID uint32, mill int64) error {
	key := "nid:" + strconv.Itoa(s.id) + ":" + strconv.FormatUint(uint64(nphID), 10)
	s.logger.Tracef("key: %s; val: %d", key, mill)
	_, err := c.Do("SET", key, mill, "ex", 50)
	s.logger.Tracef("err: %v", err)
	return err
}

func readNDTPid(c redis.Conn, s *session, nphID uint32) (int64, error) {
	key := "nid:" + strconv.FormatUint(uint64(s.id), 10) + ":" + strconv.FormatUint(uint64(nphID), 10)
	res, err := redis.Int64(c.Do("GET", key))
	s.logger.Tracef("key: %s; res: %d", key, res)
	s.logger.Tracef("err: %v", err)
	return res, err
}

func writeNDTPIdExt(c redis.Conn, s *session, mesID uint16, mill int64) error {
	key := "ext_id_c:" + strconv.Itoa(s.id) + ":" + strconv.FormatUint(uint64(mesID), 10)
	s.logger.Tracef("key: %s; val: %d", key, mill)
	_, err := c.Do("SET", key, mill, "ex", 50)
	s.logger.Tracef("err: %v", err)
	return err
}

func readNDTPIdExt(c redis.Conn, s *session, mesID uint16) (int64, error) {
	key := "ext_id_c:" + strconv.Itoa(s.id) + ":" + strconv.FormatUint(uint64(mesID), 10)
	res, err := redis.Int64(c.Do("GET", key))
	s.logger.Tracef("key: %s; res: %d", key, res)
	s.logger.Tracef("err: %v", err)
	return res, err
}

func write2DB(c redis.Conn, data ndtpData, s *session, packet []byte, time int64) (err error) {
	err = write2NDTP(c, s, time, packet)
	s.logger.Tracef("packet: %v", packet)
	s.logger.Tracef("time: %d, err: %v", time, err)
	if err != nil {
		return
	}
	if data.ToRnis.time != 0 {
		err = write2EGTS(c, s, time, packet)
		s.logger.Tracef("time: %d, err: %v", time, err)
	}
	return
}

func write2NDTP(c redis.Conn, s *session, time int64, packet []byte) error {
	s.logger.Tracef("time: %d", time)
	_, err := c.Do("ZADD", s.id, time, packet)
	s.logger.Tracef("write2NDTP err: %v", err)
	return err
}

func write2EGTS(c redis.Conn, s *session, time int64, packet []byte) error {
	idB := new(bytes.Buffer)
	binary.Write(idB, binary.LittleEndian, uint32(s.id))
	packet = append(idB.Bytes(), packet...)
	s.logger.Tracef("time: %d", time)
	_, err := c.Do("ZADD", "rnis", time, packet)
	return err
}

func removeFromNDTP(c redis.Conn, s *session, NPHReqID uint32) error {
	time, err := readNDTPid(c, s, NPHReqID)
	if err != nil {
		return err
	}
	s.logger.Tracef("id: %d; time: %d", s.id, time)
	n, err := c.Do("ZREMRANGEBYSCORE", s.id, time, time)
	s.logger.Tracef("n=%d; err: %v", n, err)
	return err
}

func removeFromNDTPExt(c redis.Conn, s *session, mesID uint16) error {
	time, err := readNDTPIdExt(c, s, mesID)
	if err != nil {
		return err
	}
	key := "ext_c:" + strconv.Itoa(s.id)
	s.logger.Tracef("key: %s; time: %d", key, time)
	n, err := c.Do("ZREMRANGEBYSCORE", key, time, time)
	s.logger.Tracef("n=%d; err: %v", n, err)
	return err
}

func getScore(c redis.Conn, s *session, mes []byte) (int64, error) {
	s.logger.Tracef("mes: %v", mes)
	res, err := redis.Int64(c.Do("ZSCORE", s.id, mes))
	s.logger.Tracef("res=%d; err: %v", res, mes)
	return res, err
}

func getOldNDTP(c redis.Conn, s *session) ([][]byte, error) {
	max := getMill() - 60000
	res, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", s.id, 0, max, "LIMIT", 0, 1000))
	s.logger.Tracef("max: %d; len(res): %d", max, len(res))
	s.logger.Tracef("err: %v", err)
	return res, err
}

func writeEGTSid(c redis.Conn, egtsMessageID uint16, messageID string, logger *logrus.Entry) (err error) {
	key := "egts:" + strconv.Itoa(int(egtsMessageID))
	_, err = c.Do("SET", key, messageID, "ex", 50)
	logger.Tracef("key: %v; messageID: %v", key, messageID)
	logger.Tracef("err: %v", err)
	return
}

func deleteEGTS(c redis.Conn, egtsMessageID uint16, logger *logrus.Entry) (err error) {
	key := "egts:" + strconv.Itoa(int(egtsMessageID))
	messageID, err := redis.String(c.Do("GET", key))
	logger.Tracef("key: %s; messageID: %s", key, messageID)
	if err != nil {
		logger.Tracef("can't get EGTS message id from db: ", err)
		return
	}
	logger.Tracef("get messageID: ", messageID)
	messageIDSplit := strings.Split(messageID, ":")
	logger.Tracef("messageIDSplit: ", messageIDSplit)
	id, err := strconv.ParseUint(messageIDSplit[0], 10, 32)
	logger.Tracef("id: %d, err: %v", id, err)
	time, err := strconv.ParseInt(messageIDSplit[1], 10, 64)
	logger.Tracef("time: %d, err: %v", time, err)
	packets, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", "rnis", time, time))
	logger.Tracef("key: %s; mssageID: %d", key, time)
	if err != nil {
		logger.Tracef("can't get EGTS packets from db: ", err)
		return
	}
	numPackets := len(packets)
	logger.Tracef("len(packets) = %d", numPackets)
	switch {
	case numPackets > 1:
		for _, pack := range packets {
			logger.Tracef("bytes1: %v; bytes2: %v", id, binary.LittleEndian.Uint32(pack[0:4]))
			//if bytes.Compare(pack[0:4], idB.Bytes()) == 0 {
			if id == uint64(binary.LittleEndian.Uint32(pack[0:4])) {
				var n int
				n, err = redis.Int(c.Do("ZREM", "rnis", pack))
				if err != nil {
					logger.Tracef("can't delete EGTS packet from db")
				}
				logger.Tracef("removed %d records", n)
				return
			}
		}
	case numPackets == 1:
		_, err = c.Do("ZREM", "rnis", packets[0])
		if err != nil {
			logger.Tracef("can't delete EGTS packet from db")
		}
	default:
		logger.Tracef("where is no EGTS packets for ", egtsMessageID)
		return
	}
	logger.Tracef("err: %v", err)
	return
}

func removeExpiredDataEGTS(c redis.Conn, logger *logrus.Entry) (err error) {
	max := getMill() - 259200000 //3*24*60*60*1000
	_, err = c.Do("ZREMRANGEBYSCORE", "rnis", 0, max)
	logger.Tracef("max: %d", max)
	logger.Tracef("err: %v", err)
	return
}

func getOldEGTS(c redis.Conn, logger *logrus.Entry) (res [][]byte, err error) {
	max := getMill() - 60000
	res, err = redis.ByteSlices(c.Do("ZRANGEBYSCORE", "rnis", 0, max, "LIMIT", 0, 10000))
	logger.Tracef("max: %d; len(res): %d", max, len(res))
	logger.Tracef("err: %v", err)
	return
}

func getEGTSScore(c redis.Conn, mes []byte, logger *logrus.Entry) (int64, error) {
	logger.Tracef("mes: %v", mes)
	res, err := redis.Int64(c.Do("ZSCORE", "rnis", mes))
	logger.Tracef("res=%d; err: %v", res, err)
	return res, err
}

func writeControlID(c redis.Conn, s *session, id1 int, id2 uint32) error {
	key := "gc_s:" + strconv.Itoa(s.id) + ":" + strconv.Itoa(id1)
	s.logger.Tracef("key: %s; val: %v", key, id2)
	val := strconv.Itoa(int(id2))
	_, err := c.Do("SET", key, val, "ex", 50)
	s.logger.Tracef("err: %v", err)
	return err
}

func readControlID(c redis.Conn, s *session, id1 int) (int, error) {
	key := "gc_s:" + strconv.Itoa(s.id) + ":" + strconv.Itoa(id1)
	res, err := redis.Int(c.Do("GET", key))
	s.logger.Tracef("key: %s; res: %d", key, res)
	s.logger.Tracef("err: %v", err)
	return res, err
}

func removeExpiredDataNDTP(c redis.Conn, s *session) (err error) {
	max := getMill() - 259200000 //3*24*60*60*1000
	_, err = c.Do("ZREMRANGEBYSCORE", s.id, 0, max)
	s.logger.Tracef("err: %v", err)
	if err != nil {
		return
	}
	key := "ext_s:" + strconv.Itoa(s.id)
	_, err = c.Do("ZREMRANGEBYSCORE", key, 0, max)
	s.logger.Tracef(" max: %d", max)
	s.logger.Tracef("err: %v", err)
	if err != nil {
		return
	}
	return
}

func writeExtServ(c redis.Conn, s *session, packet []byte, mill int64, mesID uint16) error {
	key := "ext_s:" + strconv.Itoa(s.id)
	time := strconv.FormatInt(mill, 10)
	flag := "0"
	s.logger.Tracef("time: %s", time)
	_, err := c.Do("HMSET", key, "time", time, "flag", flag, "mesID", mesID, "packet", packet)
	s.logger.Tracef("err: %v", err)
	return err
}

func removeServerExt(c redis.Conn, s *session) error {
	key := "ext_s:" + strconv.Itoa(s.id)
	n, err := redis.Int(c.Do("DEL", key))
	s.logger.Tracef("remove n records: %d;", n)
	s.logger.Tracef("err: %v;", err)
	return err
}

func getServExt(c redis.Conn, s *session) (mes []byte, time int64, flag string, mesID uint64, err error) {
	key := "ext_s:" + strconv.Itoa(s.id)
	s.logger.Tracef("key: %s", key)
	res, err := redis.StringMap(c.Do("HGETALL", key))
	if err != nil {
		s.logger.Tracef("err: %v", err)
		return
	}
	if len(res) == 0 {
		s.logger.Tracef("res for %s is empty", key)
		err = fmt.Errorf("res is empty for %s", key)
		return
	}
	flag = res["flag"]
	time, err = strconv.ParseInt(res["time"], 10, 64)
	if err != nil {
		s.logger.Tracef("parse time err: %v", err)
	}
	mes = []byte(res["packet"])
	mesID, err = strconv.ParseUint(res["mesID"], 10, 16)
	if err != nil {
		s.logger.Tracef("parse mesID err: %v", err)
	}
	return
}

func setFlagServerExt(c redis.Conn, s *session, flag string) error {
	key := "ext_s:" + strconv.Itoa(s.id)
	exist, err := redis.Int(c.Do("EXISTS", key))
	if err != nil {
		s.logger.Tracef("can't check existence of key: %v", key)
		return err
	}
	s.logger.Tracef("id exists: %v;", exist)
	if exist == 1 {
		_, err = c.Do("HSET", key, "flag", flag)
		s.logger.Tracef("err: %v;", err)
	} else {
		s.logger.Tracef("packet %s doesn't exist", key)
		err = fmt.Errorf("record doesn't exist: %v", err)
	}
	return err
}

func writeExtClient(c redis.Conn, s *session, time int64, packet []byte) error {
	key := "ext_c:" + strconv.Itoa(s.id)
	s.logger.Tracef("id: %s; time: %d", key, time)
	_, err := c.Do("ZADD", key, time, packet)
	s.logger.Tracef("err: %v", err)
	return err
}

func getOldNDTPExt(c redis.Conn, s *session) ([][]byte, error) {
	key := "ext_c:" + strconv.Itoa(s.id)
	max := getMill() - 60000
	res, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", key, 0, max, "LIMIT", 0, 10))
	s.logger.Tracef("id: %s; max: %d; len(res): %d", key, max, len(res))
	s.logger.Tracef("err: %v", err)
	return res, err
}

func getScoreExt(c redis.Conn, s *session, mes []byte) (int64, error) {
	key := "ext_c:" + strconv.Itoa(s.id)
	s.logger.Tracef("key=%s; mes: %v", key, mes)
	res, err := redis.Int64(c.Do("ZSCORE", key, mes))
	s.logger.Tracef("res=%d; err: %v", res, mes)
	return res, err
}

func removeOldExt(c redis.Conn, s *session, time int64) error {
	key := "ext_c:" + strconv.Itoa(s.id)
	s.logger.Tracef("key: %s; time: %d", key, time)
	n, err := c.Do("ZREMRANGEBYSCORE", key, time, time)
	s.logger.Tracef("n=%d; err: %v", n, err)
	return err
}
