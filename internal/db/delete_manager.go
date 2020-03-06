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
	Chan chan *ConfMsg
	dbConn Conn
	toDelete map[string]uint64
	deleted map[string]bool
	logger *logrus.Entry
	all uint64
}

// ConfMsg is type of messages from client to delete manager
type ConfMsg struct {
	key   []byte
	sysID byte
}

// InitDeleteManager initializes delete manager
func InitDeleteManager(db string, systemIds []byte) *DeleteManager {
	Manager := new(DeleteManager)
	Manager.dbConn = Connect(db)
	Manager.Chan = make(chan *ConfMsg, DeleteChanSize)
	Manager.logger = logrus.WithFields(logrus.Fields{"type": "delete_manager"})
	Manager.all = calcAll(systemIds)
	Manager.toDelete = make(map[string]uint64)
	Manager.deleted = make(map[string]bool)
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

func (m *DeleteManager) receiveLoop() {
	for {
		select {
		case message := <- m.Chan:
			err := m.handleMessage(message)
			if err != nil {
				m.logger.Errorf("can't delete message: %s", err)
			}
		}
	}
}

func (m *DeleteManager) handleMessage(message *ConfMsg) (err error) {
	msg := string(message.key)
	if _, ok := m.deleted[msg]; ok {
		return
	}
	if val, ok := m.toDelete[msg]; ok {
		return m.handleExisted(msg, message, val)
	}
	return m.handleNotExisted(msg, message)
}

func (m *DeleteManager) handleExisted(msg string, message *ConfMsg, val uint64) (err error) {
	val |= (1 << message.sysID)
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
	if existsInDB(m.dbConn, message.key) {
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
	err = deletePacket(m.dbConn, message.key)
	if err != nil {
		m.deleted[msg] = true
		delete(m.toDelete, msg)
		return
	}
	m.deleted[msg] = true
	return
}

func (m *DeleteManager) markSysConfirmed(message *ConfMsg) error {
	_, err := m.dbConn.Do("SETBIT", message.key, message.sysID, 1)
	return err
}

func (m *DeleteManager) countBits(key []byte) (int, error) {
	n, err := redis.Int(m.dbConn.Do("BITCOUNT", key, 0, systemBytes-1))
	return n, err
}

func deletePacket(conn redis.Conn, key []byte) error {
	packet, err := findPacket(conn, key)
	if err != nil {
		return err
	}
	terminalID := util.TerminalID(key)
	_, err = conn.Do("ZREM", util.EgtsName, key)
	if err != nil {
		return err
	}
	_, err = conn.Do("ZREM", terminalID, packet)
	if err != nil {
		return err
	}
	_, err = conn.Do("DEL", key)
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