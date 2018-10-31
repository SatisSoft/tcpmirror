package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"log"
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
	key := "nid:" + strconv.Itoa(id) + ":" + strconv.FormatUint(uint64(nphID), 10)
	log.Printf("writeNDTPid key: %s; val: %d", key, mill)
	_, err := c.Do("SET", key, mill, "ex", 50)
	log.Printf("writeNDTPid err: %v", err)
	return err
}

func readNDTPid(c redis.Conn, id int, nphID uint32) (int64, error) {
	key := "nid:" + strconv.FormatUint(uint64(id), 10) + ":" + strconv.FormatUint(uint64(nphID), 10)
	res, err := redis.Int64(c.Do("GET", key))
	log.Printf("readNDTPid key: %s; res: %d", key, res)
	log.Printf("readNDTPid err: %v", err)
	return res, err
}

func writeNDTPIdExt(c redis.Conn, id int, mesID uint16, mill int64) error {
	key := "ext_id_c:" + strconv.Itoa(id) + ":" + strconv.FormatUint(uint64(mesID), 10)
	log.Printf("writeNDTPIdExt key: %s; val: %d", key, mill)
	_, err := c.Do("SET", key, mill, "ex", 50)
	log.Printf("writeNDTPIdExt err: %v", err)
	return err
}

func readNDTPIdExt(c redis.Conn, id int, mesID uint16) (int64, error) {
	key := "ext_id_c:" + strconv.Itoa(id) + ":" + strconv.FormatUint(uint64(mesID), 10)
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
	key := "ext_c:" + strconv.Itoa(id)
	log.Printf("removeFromNDTPExt: key: %s; time: %d", key, time)
	n, err := c.Do("ZREMRANGEBYSCORE", key, time, time)
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
		log.Println("deleteEGTS: error get EGTS message id from db: ", err)
		return
	}
	log.Println("deleteEGTS: get messageID: ", messageID)
	messageIDSplit := strings.Split(messageID, ":")
	log.Println("deleteEGTS: messageIDSplit: ", messageIDSplit)
	id, err := strconv.ParseUint(messageIDSplit[0], 10, 32)
	log.Printf("deleteEGTS: id: %d, err: %v", id, err)
	time, err := strconv.ParseInt(messageIDSplit[1], 10, 64)
	log.Printf("deleteEGTS: time: %d, err: %v", time, err)
	//idB := new(bytes.Buffer)
	//binary.Write(idB, binary.LittleEndian, id)

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
			log.Printf("deleteEGTS: bytes1: %v; bytes2: %v", id, binary.LittleEndian.Uint32(pack[0:4]))
			//if bytes.Compare(pack[0:4], idB.Bytes()) == 0 {
			if id == uint64(binary.LittleEndian.Uint32(pack[0:4])) {
				var n int
				n, err = redis.Int(c.Do("ZREM", "rnis", pack))
				if err != nil {
					log.Println("deleteEGTS: error while deleting EGTS packet from db")
				}
				log.Printf("deleteEGTS: removed %d records", n)
				return
			}
		}
	case numPackets == 1:
		_, err = c.Do("ZREM", "rnis", packets[0])
		if err != nil {
			log.Println("deleteEGTS: error while deleting EGTS packet from db")
		}
	default:
		log.Println("deleteEGTS: where is no EGTS packets for ", egtsMessageID)
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

func getEGTSScore(c redis.Conn, mes []byte)(int64, error){
	log.Printf("getEGTSScore mes: %v", mes)
	res, err := redis.Int64(c.Do("ZSCORE", "rnis", mes))
	log.Printf("getEGTSScore res=%d; err: %v", res, err)
	return res, err
}

func writeControlID(c redis.Conn, id, id1 int, id2 uint32) error {
	key := "gc_s:" + strconv.Itoa(id) + ":" + strconv.Itoa(id1)
	log.Printf("writeControlID key: %s; val: %v", key, id2)
	val := strconv.Itoa(int(id2))
	_, err := c.Do("SET", key, val, "ex", 50)
	log.Printf("writeControlID err: %v", err)
	return err
}

func readControlID(c redis.Conn, id, id1 int) (int, error) {
	key := "gc_s:" + strconv.Itoa(id) + ":" + strconv.Itoa(id1)
	res, err := redis.Int(c.Do("GET", key))
	log.Printf("readControlID key: %s; res: %d", key, res)
	log.Printf("readControlID err: %v", err)
	return res, err
}

func removeExpiredDataNDTP(c redis.Conn, id int) (err error) {
	max := getMill() - 259200000 //3*24*60*60*1000
	_, err = c.Do("ZREMRANGEBYSCORE", id, 0, max)
	log.Printf("removeExpiredDataNDTP err: %v", err)
	if err != nil {
		return
	}
	key := "ext_s:" + strconv.Itoa(id)
	_, err = c.Do("ZREMRANGEBYSCORE", key, 0, max)
	log.Printf("removeExpiredDataNDTP: max: %d", max)
	log.Printf("removeExpiredDataNDTP err1: %v", err)
	if err != nil {
		return
	}
	return
}

func writeExtServ(c redis.Conn, id int, packet []byte, mill int64, mesID uint16) error {
	key := "ext_s:" + strconv.Itoa(id)
	time := strconv.FormatInt(mill, 10)
	flag := "0"
	log.Printf("writeExtServ id: %d; time: %s", id, time)
	_, err := c.Do("HMSET", key, "time", time, "flag", flag, "mesID", mesID, "packet", packet)
	log.Printf("writeExtServ err: %v", err)
	return err
}

func removeServerExt(c redis.Conn, id int) error {
	key := "ext_s:" + strconv.Itoa(id)
	log.Printf("removeServerExt id: %d;", id)
	_, err := c.Do("DEL", key)
	log.Printf("removeServerExt err: %v;", err)
	return err
}

//func setNDTPExtFlag(c redis.Conn, id int, flag string) error {
//	key := "ext_s:" + strconv.Itoa(id)
//	log.Printf("setServExtFlag key: %s; flag: %s", key, flag)
//	_, err := c.Do("HSET", key, flag, flag)
//	return err
//}

func getServExt(c redis.Conn, id int) (mes []byte, time int64, flag string, mesID uint64, err error) {
	key := "ext_s:" + strconv.Itoa(id)
	log.Printf("getServExt key: %s", key)
	res, err := redis.StringMap(c.Do("HGETALL", key))
	if err != nil {
		log.Printf("getServExt error: %v", err)
		return
	}
	if len(res) == 0 {
		log.Printf("res for %s is empty", key)
		err = fmt.Errorf("getServExt res is empty for %s", key)
		return
	}
	flag = res["flag"]
	time, err = strconv.ParseInt(res["time"], 10, 64)
	if err != nil {
		log.Printf("getServExt parse time error: %v", err)
	}
	mes = []byte(res["packet"])
	mesID, err = strconv.ParseUint(res["mesID"], 10, 16)
	if err != nil {
		log.Printf("getServExt parse mesID error: %v", err)
	}
	return
}

func setFlagServerExt(c redis.Conn, id int, flag string) error {
	key := "ext_s:" + strconv.Itoa(id)
	log.Printf("setFlagServerExt: id %d;", id)
	exist, err := redis.Int(c.Do("EXISTS", key))
	if err != nil {
		log.Printf("setFlagServerExt: error checking existance for key: %v", key)
		return err
	}
	log.Printf("setFlagServerExt: id exist: %v;", exist)
	log.Printf("setFlagServerExt: f: %d %[1]T; f1: %d %[2]T; is equal: %t", exist, 1, exist == 1)
	if exist == 1 {
		_, err = c.Do("HSET", key, "flag", flag)
		log.Printf("setFlagServerExt: err2: %v;", err)
	} else {
		log.Printf("setFlagServerExt: packet %s doesn't exist", key)
		err = fmt.Errorf("setFlagServerExt: record doesn't exist: %v", err)
	}
	return err
}

func writeExtClient(c redis.Conn, id int, time int64, packet []byte) error {
	key := "ext_c:" + strconv.Itoa(id)
	log.Printf("writeExtClient id: %s; time: %d", key, time)
	_, err := c.Do("ZADD", key, time, packet)
	log.Printf("writeExtClient err: %v", err)
	return err
}

func getOldNDTPExt(c redis.Conn, id int) ([][]byte, error) {
	key := "ext_c:" + strconv.Itoa(id)
	max := getMill() - 60000
	res, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", key, 0, max, "LIMIT", 0, 10))
	log.Printf("getOldNDTPExt: id: %s; max: %d; len(res): %d", key, max, len(res))
	log.Printf("getOldNDTPExt err: %v", err)
	return res, err
}

func getScoreExt(c redis.Conn, id int, mes []byte) (int64, error) {
	key := "ext_c:" + strconv.Itoa(id)
	log.Printf("getScoreExt key=%s; mes: %v", key, mes)
	res, err := redis.Int64(c.Do("ZSCORE", key, mes))
	log.Printf("getScoreExt res=%d; err: %v", res, mes)
	return res, err
}

func removeOldExt(c redis.Conn, id int, time int64) error {
	key := "ext_c:" + strconv.Itoa(id)
	log.Printf("removeOldExt: key: %s; time: %d", key, time)
	n, err := c.Do("ZREMRANGEBYSCORE", key, time, time)
	log.Printf("removeOldExt n=%d; err: %v", n, err)
	return err
}
