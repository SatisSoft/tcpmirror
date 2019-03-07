package main

import (
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
)

type session struct {
	clientNPLReqID uint16
	clientNPHReqID uint32
	serverNPLReqID uint16
	serverNPHReqID uint32
	muRecon        sync.Mutex
	muC            sync.Mutex
	muS            sync.Mutex
	id             int
	logger         *logrus.Entry
	errClientCh    chan error
	clientConn     net.Conn
	servConn       connection
	redisConn      redis.Conn
	muRedis        sync.Mutex
}

func newSession(c net.Conn, connNo uint64) (s *session, err error) {
	s = new(session)
	s.clientConn = c
	s.logger = logrus.WithFields(logrus.Fields{"connNum": connNo})
	s.errClientCh = make(chan error)
	s.servConn = connection{ndtpServer, nil, true, false}
	if s.redisConn, err = redis.Dial("tcp", redisServer); err != nil {
		return
	}
	return
}

func closeSession(s *session) {
	s.logger.Debugln("close session")
	err := s.clientConn.Close()
	if err != nil {
		s.logger.Errorf("error closing client connection: %s", err)
	}
	if !s.servConn.closed {
		s.logger.Printf("close connection to server")
		err = s.servConn.conn.Close()
		if err != nil {
			s.logger.Errorf("error closing server connection: %s", err)
		}
	}
	return
}

func (s *session) setID(id int) {
	s.id = id
	s.logger = s.logger.WithField("id", id)
}

func (s *session) serverNplID() uint16 {
	s.muS.Lock()
	nplID := s.serverNPLReqID
	s.serverNPLReqID++
	s.muS.Unlock()
	return nplID
}

func (s *session) serverNphID() uint32 {
	s.muS.Lock()
	nphID := s.serverNPHReqID
	s.serverNPHReqID++
	s.muS.Unlock()
	return nphID
}

func (s *session) clientNplID() uint16 {
	s.muC.Lock()
	nplID := s.clientNPLReqID
	s.clientNPLReqID++
	s.muC.Unlock()
	return nplID
}

func (s *session) clientNphID() uint32 {
	s.muC.Lock()
	nphID := s.clientNPHReqID
	s.clientNPHReqID++
	s.muC.Unlock()
	return nphID
}

type egtsSession struct {
	egtsMessageID uint16
	egtsRecID     uint16
	mu            sync.Mutex
}

func (s *egtsSession) ids() (uint16, uint16) {
	s.mu.Lock()
	egtsMessageID := s.egtsMessageID
	egtsRecID := s.egtsRecID
	s.egtsMessageID++
	s.egtsRecID++
	s.mu.Unlock()
	return egtsMessageID, egtsRecID
}
