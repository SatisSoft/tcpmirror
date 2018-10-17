package main

import (
	"bytes"
	"encoding/binary"
	"github.com/gomodule/redigo/redis"
	"log"
	"strconv"
	"strings"
)

func writeConnDB(c redis.Conn, id uint32, message []byte) error {
	key := "conn:" + strconv.Itoa(int(id))
	log.Printf("writeConnDB key: %s; message: %v", key, message)
	_, err := c.Do("SET", key, message)
	log.Printf("writeConnDB err: %v", err)
	return err
}

func readConnDB(c redis.Conn, id int) ([]byte, error) {
	key := "conn:" + strconv.Itoa(id)
	res, err := redis.Bytes(c.Do("GET", key))
	log.Printf("readConnDB key: %s; message: %v", key, res)
	log.Printf("readConnDB err: %v", err)
	return res, err
}

func writeNDTPid(c redis.Conn, id int, nphID uint32, mill int64) error {
	key := "ntn:" + strconv.Itoa(id) + ":" + strconv.FormatUint(uint64(nphID), 10)
	log.Printf("writeNDTPid key: %s; val: %d", key, mill)
	_, err := c.Do("SET", key, mill, "ex", 50)
	log.Printf("writeNDTPid err: %v", err)
	return err
}

func readNDTPid(c redis.Conn, id int, nphID uint32) (int64, error) {
	key := "ntn:" + strconv.FormatUint(uint64(id), 10) + ":" + strconv.FormatUint(uint64(nphID), 10)
	res, err := redis.Int64(c.Do("GET", key))
	log.Printf("readNDTPid key: %s; res: %d", key, res)
	log.Printf("readNDTPid err: %v", err)
	return res, err
}

func writeNDTPIdExt(c redis.Conn, id int, mesID uint16, mill int64) error {
	key := "ext:" + strconv.Itoa(id) + ":" + strconv.FormatUint(uint64(mesID), 10)
	log.Printf("writeNDTPIdExt key: %s; val: %d", key, mill)
	_, err := c.Do("SET", key, mill, "ex", 50)
	log.Printf("writeNDTPIdExt err: %v", err)
	return err
}

func readNDTPIdExt(c redis.Conn, id int, mesID uint16) (int64, error) {
	key := "ext:" + strconv.Itoa(id) + ":" + strconv.FormatUint(uint64(mesID), 10)
	res, err := redis.Int64(c.Do("GET", key))
	log.Printf("readNDTPIdExt key: %s; res: %d", key, res)
	log.Printf("readNDTPIdExt err: %v", err)
	return res, err
}

func write2DB(c redis.Conn, data ndtpData, s *session, packet []byte, time int64) (err error) {
	err = write2NDTP(c, s.id, time, packet)
	log.Printf("write2DB packet: %v", packet)
	log.Printf("write2NDTP id: %d, time: %d, err: %v", s.id, time, err)
	if err != nil {
		return
	}
	if data.ToRnis.time != 0 {
		err = write2EGTS(c, s.id, time, packet)
		log.Printf("write2DB id: %d, time: %d, err: %v", s.id, time, err)
	}
	return
}

func write2NDTP(c redis.Conn, id int, time int64, packet []byte) error {
	//log.Printf("write2NDTP id: %d; time: %d; packet: %v", id, time, packet)
	log.Printf("write2NDTP id: %d; time: %d", id, time)
	_, err := c.Do("ZADD", id, time, packet)
	log.Printf("write2NDTP err: %v", err)
	return err
}

func write2EGTS(c redis.Conn, id int, time int64, packet []byte) error {
	idB := new(bytes.Buffer)
	binary.Write(idB, binary.LittleEndian, uint32(id))
	packet = append(idB.Bytes(), packet...)
	//log.Printf("write2EGTS id: %d; time: %d; packet: %v", id, time, packet)
	log.Printf("write2EGTS id: %d; time: %d", id, time)
	_, err := c.Do("ZADD", "rnis", time, packet)
	return err
}

func removeFromNDTP(c redis.Conn, id int, NPHReqID uint32) error {
	time, err := readNDTPid(c, id, NPHReqID)
	if err != nil {
		return err
	}
	log.Printf("removeFromNDTP: id: %d; time: %d", id, time)
	n, err := c.Do("ZREMRANGEBYSCORE", id, time, time)
	log.Printf("removeFromNDTP n=%d; err: %v", n, err)
	return err
}

func removeFromNDTPExt(c redis.Conn, id int, mesID uint16) error {
	time, err := readNDTPIdExt(c, id, mesID)
	if err != nil {
		return err
	}
	log.Printf("removeFromNDTPExt: id: %d; time: %d", id, time)
	n, err := c.Do("ZREMRANGEBYSCORE", id, time, time)
	log.Printf("removeFromNDTPExt n=%d; err: %v", n, err)
	return err
}

func getScore(c redis.Conn, id int, mes []byte) (int64, error) {
	log.Printf("getScore id=%d; mes: %v", id, mes)
	res, err := redis.Int64(c.Do("ZSCORE", id, mes))
	log.Printf("getScore res=%d; err: %v", res, mes)
	return res, err
}

func getOldNDTP(c redis.Conn, id int) ([][]byte, error) {
	max := getMill() - 60000
	res, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", id, 0, max, "LIMIT", 0, 10))
	log.Printf("getOldNDTP: id: %d; max: %d; len(res): %d", id, max, len(res))
	log.Printf("getOldNDTP err: %v", err)
	return res, err
}

func writeEGTSid(c redis.Conn, egtsMessageID uint16, messageID string) (err error) {
	key := "egts:" + strconv.Itoa(int(egtsMessageID))
	//log.Printf("writeEGTSid: key: %s; messageID: %s", key, messageID)
	_, err = c.Do("SET", key, messageID, "ex", 50)
	log.Printf("writeEGTSid err: %v", err)
	return
}

func deleteEGTS(c redis.Conn, egtsMessageID uint16) (err error) {
	key := "egts:" + strconv.Itoa(int(egtsMessageID))
	messageID, err := redis.String(c.Do("GET", key))
	log.Printf("deleteEGTS 1: key: %s; mssageID: %s", key, messageID)
	if err != nil {
		log.Println("error get EGTS message id from db: ", err)
		return
	}
	log.Println("get messageID: ", messageID)
	messageIDSplit := strings.Split(messageID, ":")
	log.Println("messageIDSplit: ", messageIDSplit)
	id, err := strconv.ParseUint(messageIDSplit[1], 10, 32)
	time, err := strconv.ParseInt(messageIDSplit[1], 10, 64)
	idB := new(bytes.Buffer)
	binary.Write(idB, binary.LittleEndian, id)

	packets, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", "rnis", time, time))
	log.Printf("deleteEGTS 2: key: %s; mssageID: %d", key, time)
	if err != nil {
		log.Println("error get EGTS packets from db: ", err)
		return
	}
	numPackets := len(packets)
	log.Printf("deleteEGTS 3: len(packets) = %d", numPackets)
	switch {
	case numPackets > 1:
		for _, pack := range packets {
			if bytes.Compare(pack[0:4], idB.Bytes()) == 0 {
				_, err = c.Do("ZREM", "rnis", pack)
				if err != nil {
					log.Println("error while deleting EGTS packet from db")
				}
				log.Println("where is no EGTS packets for EGTSMessageID: ", egtsMessageID, "; messageID: ", messageID)
				return
			}
		}
	case numPackets == 1:
		_, err = c.Do("ZREM", "rnis", packets[0])
		if err != nil {
			log.Println("error while deleting EGTS packet from db")
		}
	default:
		log.Println("where is no EGTS packets for ", egtsMessageID)
		return
	}
	log.Printf("deleteEGTS err: %v", err)
	return
}

func removeExpiredDataEGTS(c redis.Conn) (err error) {
	max := getMill() - 259200000 //3*24*60*60*1000
	_, err = c.Do("ZREMRANGEBYSCORE", "rnis", 0, max)
	log.Printf("removeExpiredDataEGTS: max: %d", max)
	log.Printf("removeExpiredDataEGTS err: %v", err)
	return
}

func getOldEGTS(c redis.Conn) (res [][]byte, err error) {
	max := getMill() - 60000
	res, err = redis.ByteSlices(c.Do("ZRANGEBYSCORE", "rnis", 0, max, "LIMIT", 0, 10000))
	log.Printf("getOldEGTS: max: %d; len(res): %d", max, len(res))
	log.Printf("getOldEGTS err: %v", err)
	return
}

func writeControlID(c redis.Conn, id, id1 int, id2 uint32) error {
	key := "s2c:" + strconv.Itoa(id) + ":" + strconv.Itoa(id1)
	log.Printf("writeControlID key: %s; val: %v", key, id2)
	val := strconv.Itoa(int(id2))
	_, err := c.Do("SET", key, val, "ex", 30)
	log.Printf("writeControlID err: %v", err)
	return err
}

func readControlID(c redis.Conn, id, id1 int) (int, error) {
	key := "s2c:" + strconv.Itoa(id) + ":" + strconv.Itoa(id1)
	res, err := redis.Int(c.Do("GET", key))
	log.Printf("readControlID key: %s; res: %d", key, res)
	log.Printf("readControlID err: %v", err)
	return res, err
}

func write2DBServer(c redis.Conn, s *session, packet []byte, time int64) error {
	key := "serv:" + strconv.Itoa(s.id)
	_, err := c.Do("ZADD", key, time, packet)
	log.Printf("write2DBServer packet: %v", packet)
	log.Printf("write2DBServer id: %d, time: %d, err: %v", s.id, time, err)
	return err
}

func removeFromNDTPExtServ(c redis.Conn, id int, mesID uint16) error {
	time, err := readNDTPIdExtServ(c, id, mesID)
	if err != nil {
		return err
	}
	log.Printf("removeFromNDTPExtServ: id: %d; time: %d", id, time)
	key := "serv:" + strconv.Itoa(id)
	n, err := c.Do("ZREMRANGEBYSCORE", key, time, time)
	log.Printf("removeFromNDTPExtServ n=%d; err: %v", n, err)
	return err
}

func writeNDTPIdExtServ(c redis.Conn, id int, mesID uint16, mill int64) error {
	key := "s_ext:" + strconv.Itoa(id) + ":" + strconv.FormatUint(uint64(mesID), 10)
	log.Printf("writeNDTPIdExtServ key: %s; val: %d", key, mill)
	_, err := c.Do("SET", key, mill, "ex", 50)
	log.Printf("writeNDTPIdExtServ err: %v", err)
	return err
}

func readNDTPIdExtServ(c redis.Conn, id int, mesID uint16) (int64, error) {
	key := "s_ext:" + strconv.Itoa(id) + ":" + strconv.FormatUint(uint64(mesID), 10)
	res, err := redis.Int64(c.Do("GET", key))
	log.Printf("readNDTPIdExtServ key: %s; res: %d", key, res)
	log.Printf("readNDTPIdExtServ err: %v", err)
	return res, err
}

func getOldNDTPServ(c redis.Conn, id int) ([][]byte, error) {
	max := getMill() - 60000
	key := "serv:" + strconv.Itoa(id)
	res, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", key, 0, max, "LIMIT", 0, 10))
	log.Printf("getOldNDTPServ: id: %d; max: %d; len(res): %d", id, max, len(res))
	log.Printf("getOldNDTPServ err: %v", err)
	return res, err
}

func getScoreServ(c redis.Conn, id int, mes []byte) (int64, error) {
	key := "serv:" + strconv.Itoa(id)
	log.Printf("getScoreServ id=%d; mes: %v", id, mes)
	res, err := redis.Int64(c.Do("ZSCORE", key, mes))
	log.Printf("getScoreServ res=%d; err: %v", res, mes)
	return res, err
}

func writeNDTPidServ(c redis.Conn, id int, nphID uint32, mill int64) error {
	key := "sntn:" + strconv.Itoa(id) + ":" + strconv.FormatUint(uint64(nphID), 10)
	log.Printf("writeNDTPidServ key: %s; val: %d", key, mill)
	_, err := c.Do("SET", key, mill, "ex", 50)
	log.Printf("writeNDTPidServ err: %v", err)
	return err
}

func readNDTPidServ(c redis.Conn, id int, nphID uint32) (int64, error) {
	key := "sntn:" + strconv.FormatUint(uint64(id), 10) + ":" + strconv.FormatUint(uint64(nphID), 10)
	res, err := redis.Int64(c.Do("GET", key))
	log.Printf("readNDTPidServ key: %s; res: %d", key, res)
	log.Printf("readNDTPidServ err: %v", err)
	return res, err
}

func removeExpiredDataNDTP(c redis.Conn, id int) (err error) {
	max := getMill() - 259200000 //3*24*60*60*1000
	_, err = c.Do("ZREMRANGEBYSCORE", id, 0, max)
	log.Printf("removeExpiredDataNDTP err: %v", err)
	if err != nil {
		return
	}
	key := "serv:" + strconv.Itoa(id)
	_, err = c.Do("ZREMRANGEBYSCORE", key, 0, max)
	log.Printf("removeExpiredDataNDTP: max: %d", max)
	log.Printf("removeExpiredDataNDTP err1: %v", err)
	return
}

func removeFromNDTPServ(c redis.Conn, id int, NPHReqID uint32) error {
	time, err := readNDTPidServ(c, id, NPHReqID)
	if err != nil {
		log.Printf("removeFromNDTPServ: error %s", err)
		return err
	}
	key := "serv:" + strconv.Itoa(id)
	log.Printf("removeFromNDTPServ: id: %d; time: %d", id, time)
	n, err := c.Do("ZREMRANGEBYSCORE", key, time, time)
	log.Printf("removeFromNDTPServ n=%d; err: %v", n, err)
	return err
}
