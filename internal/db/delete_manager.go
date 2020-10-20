package db

import (
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
)

// DeleteChanSize is a size of delete channel
const DeleteChanSize = 10000

// DeleteManager describes goroutine which manages deleting records from DB
type DeleteManager struct {
	Chan        chan *ConfMsg
	dbConn      Conn
	toDelete    map[string]uint64
	deleted     map[string]bool
	logger      *logrus.Entry
	all         uint64
	packetStart int
}

// ConfMsg is type of messages from client to delete manager
type ConfMsg struct {
	key   []byte
	sysID byte
}

// InitDeleteManager initializes delete manager
func InitDeleteManager(db string, systemIds []byte, serverProtocol string) *DeleteManager {
	Manager := new(DeleteManager)
	Manager.dbConn = Connect(db)
	Manager.Chan = make(chan *ConfMsg, DeleteChanSize)
	Manager.logger = logrus.WithFields(logrus.Fields{"type": "delete_manager"})
	Manager.logger.Tracef("deleteManager chan: %v", Manager.Chan)
	Manager.all = calcAll(systemIds)
	Manager.toDelete = make(map[string]uint64)
	Manager.deleted = make(map[string]bool)
	Manager.packetStart = getPacketStart(serverProtocol)
	go Manager.receiveLoop()
	return Manager
}

func calcAll(systemIds []byte) uint64 {
	all := uint64(0)
	for _, n := range systemIds {
		all |= (1 << n)
	}
	return all
}

func getPacketStart(serverProtocol string) (packetStart int) {
	if serverProtocol == "EGTS" {
		packetStart = util.PacketStartEgts
	} else {
		packetStart = util.PacketStart
	}
	return
}

func (m *DeleteManager) receiveLoop() {
	for {
		select {
		case message := <-m.Chan:
			err := m.handleMessage(message)
			if err != nil {
				m.logger.Errorf("can't delete message: %s", err)
			}
		}
	}
}

func (m *DeleteManager) handleMessage(message *ConfMsg) (err error) {
	m.logger.Tracef("handleMessage 1: %v", message)
	msg := string(message.key)
	if _, ok := m.deleted[msg]; ok {
		return
	}
	if val, ok := m.toDelete[msg]; ok {
		m.logger.Tracef("handleMessge 2: %v", val)
		return m.handleExisted(msg, message, val)
	}
	return m.handleNotExisted(msg, message)
}

func (m *DeleteManager) handleExisted(msg string, message *ConfMsg, val uint64) (err error) {
	val |= (1 << message.sysID)
	m.logger.Tracef("handleExisted 1: %v", val)
	if val == m.all {
		return m.delete(msg, message)
	}
	if err = m.markSysConfirmed(message); err != nil {
		return
	}
	m.toDelete[msg] = val
	return
}

func (m *DeleteManager) handleNotExisted(msg string, message *ConfMsg) (err error) {
	m.logger.Tracef("handeNotExisted 1: %v", msg)
	if existsInDB(m.dbConn, message.key) {
		m.logger.Tracef("handeNotExisted 2")
		var n int
		n, err = m.countBits(message.key)
		if err != nil {
			return
		}
		return m.handleExisted(msg, message, uint64(n))
	}
	m.logger.Warningf("receive %v, but key does not exist", *message)
	return
}

func (m *DeleteManager) delete(msg string, message *ConfMsg) (err error) {
	err = deletePacket(m.dbConn, message.key, m.packetStart)
	if err != nil {
		m.deleted[msg] = true
		delete(m.toDelete, msg)
		return
	}
	m.deleted[msg] = true
	return
}

func (m *DeleteManager) markSysConfirmed(message *ConfMsg) error {
	logrus.Tracef("markSysConfirmed sysID %d, key %v", message.sysID, message.key)
	_, err := m.dbConn.Do("SETBIT", message.key, message.sysID, 1)
	return err
}

func (m *DeleteManager) countBits(key []byte) (int, error) {
	n, err := redis.Int(m.dbConn.Do("BITCOUNT", key, 0, systemBytes-1))
	logrus.Tracef("countBits n = %d, key %v", n, key)
	return n, err
}

func deletePacket(conn redis.Conn, key []byte, packetStart int) error {
	packet, err := findPacket(conn, key, packetStart)
	logrus.Tracef("deletePacket key = %v, packet = %v, err = %v", key, packet, err)
	if err != nil {
		return err
	}
	res, err := conn.Do("ZREM", util.EgtsName, key)
	logrus.Tracef("del 1 res = %v, err = %v", res, err)
	if err != nil {
		return err
	}
	if util.EgtsSource != "" {
		res, err = conn.Do("ZREM", util.EgtsSource, packet)
	} else {
		terminalID := util.TerminalID(key)
		res, err = conn.Do("ZREM", terminalID, packet)
	}
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

func existsInDB(conn Conn, key []byte) bool {
	ex, _ := redis.Int(conn.Do("EXISTS", key))
	if ex == 1 {
		return true
	}
	return false
}
