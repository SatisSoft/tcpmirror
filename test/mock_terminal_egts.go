package test

import (
	"net"
	"testing"
	"time"

	"github.com/egorban/navprot/pkg/egts"
	"github.com/ashirko/tcpmirror/internal/util"
	"github.com/sirupsen/logrus"
)

func mockSourceEgts(t *testing.T, addr string, numRec int, numOIDs uint32) {
	logger := logrus.WithFields(logrus.Fields{"test": "mock_terminal"})
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer util.CloseAndLog(conn, logger)
	oid := uint32(0)
	for iRec := 0; iRec < numRec; iRec++ {
		err = sendNewMessageEgts(t, conn, iRec, oid, logger)
		oid++
		if oid == numOIDs {
			oid = 0
		}
		if err != nil {
			logger.Errorf("got error: %v", err)
			t.Error(err)
		}
	}
	time.Sleep(180 * time.Second)
}

func sendNewMessageEgts(t *testing.T, conn net.Conn, iRec int, oid uint32, logger *logrus.Entry) error {
	posData := &egts.PosData{
		Time:    1533570258 - egts.Timestamp20100101utc,
		Lon:     37.782409656276556,
		Lat:     55.62752532903746,
		Bearing: 178,
		Valid:   1,
	}

	fuelData := &egts.FuelData{
		Type: 2,
		Fuel: 2,
	}

	subrec0 := &egts.SubRecord{
		Type: egts.EgtsSrPosData,
		Data: posData,
	}

	subrec1 := &egts.SubRecord{
		Type: egts.EgtsSrLiquidLevelSensor,
		Data: fuelData,
	}
	rec := &egts.Record{
		RecNum:  uint16(iRec),
		ID:      oid,
		Service: egts.EgtsTeledataService,
		Data:    []*egts.SubRecord{subrec0, subrec1},
	}
	packetNavEgts := &egts.Packet{
		Type:    egts.EgtsPtAppdata,
		ID:      uint16(iRec),
		Records: []*egts.Record{rec},
		Data:    nil,
	}
	pack, _ := packetNavEgts.Form()
	return sendAndReceive(t, conn, pack, logger)
}
