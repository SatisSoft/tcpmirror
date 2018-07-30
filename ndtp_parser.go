package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

type ndtpData struct {
	NPLType  byte
	NPLReqID uint16
	NPH      nphData
	ToRnis   rnisData
}

type nphData struct {
	//mandatory
	ServiceID uint16
	NPHType   uint16
	NPHReqID  uint32
	NPHData   []byte
	//optional
	NPHResult uint32
	ID        uint32
}

type rnisData struct {
	Time    uint32
	Lon     uint32
	Lat     uint32
	Bearing uint16
	Speed   uint16
	Sos     bool
	ID      uint32
}

func parseNDTP(message []byte) (data ndtpData, packetLen uint16, restBuf []byte, err error) {
	index1 := bytes.Index(nplSignature, message)
	if index1 == -1 {
		err = errors.New("NPL signature not found")
		return
	}
	messageLen := len(message) - index1
	if messageLen < NPL_HEADER_LEN {
		restBuf = make([]byte, len(message))
		copy(restBuf, message)
		err = errors.New("messageLen is too short")
		return
	}
	dataLen := binary.LittleEndian.Uint16(message[index1+2 : index1+4])
	if int(dataLen) > (messageLen - NPL_HEADER_LEN) {
		restBuf = make([]byte, len(message))
		copy(restBuf, message)
		err = errors.New("messageLen is too short")
		return
	}
	if binary.LittleEndian.Uint16(message[index1+4:index1+6])&2 != 0 {
		crcHead := binary.BigEndian.Uint16(message[index1+6 : index1+8])
		crcCalc := crc16(message[index1+headerSize : index1+headerSize+int(dataLen)])
		if crcHead != crcCalc {
			err = errors.New("crc incorrect")
			return
		}
	}
	data.NPLType = message[index1+8]
	data.NPLReqID = binary.LittleEndian.Uint16(message[index1+13 : index1+15])
	err = parseNPH(message[index1+15:], &data)
	if err != nil {
		packetLen = dataLen + NPL_HEADER_LEN
		restBuf = make([]byte, len(message[NPL_HEADER_LEN+dataLen:]))
		copy(restBuf, message[NPL_HEADER_LEN+dataLen:])
	}
	return
}

func parseNPH(message []byte, data *ndtpData) (err error) {
	index := 0
	var nph nphData
	nph.ServiceID = binary.LittleEndian.Uint16(message[index : index+2])
	nph.NPHType = binary.LittleEndian.Uint16(message[index+2 : index+4])
	nph.NPHReqID = binary.LittleEndian.Uint32(message[index+6 : index+10])
	if nph.ServiceID == NPH_SRV_NAVDATA && (nph.NPHType == NPH_SND_HISTORY || nph.NPHType == NPH_SND_REALTIME) {
		rnis, NPHLen, err := parseNavData(message[index+NPH_HEADER_LEN:])
		if err != nil {
			return
		} else {
			data.ToRnis = rnis
			index = index + +NPHLen
		}
	} else {
		if nph.ServiceID == NPH_SRV_GENERIC_CONTROLS && nph.NPHType == NPH_SGC_CONN_REQUEST {
			nph.ID = binary.LittleEndian.Uint32(message[index+NPH_HEADER_LEN+6 : index+NPH_HEADER_LEN+10])
		}
		//if !stExist[servType{nph.ServiceID, nph.NPHType}] {
		//	err = fmt.Errorf("service %d Type %d doesn't exist", nph.ServiceID, nph.NPHType)
		//	return
		//}
	}
	data.NPH = nph
	return
}

func parseNavData(message []byte) (rnis rnisData, index int, err error) {
	MesLen := len(message)
	for index < MesLen-2 {
		switch Type := message[0]; Type {
		case 0:
			DataLen := navDataLength[0]
			if index+DataLen < MesLen {
				rnis.Time = binary.LittleEndian.Uint32(message[index+2 : index+6])
				rnis.Lon = binary.LittleEndian.Uint32(message[index+6 : index+10])
				rnis.Lat = binary.LittleEndian.Uint32(message[index+10 : index+14])
				if message[index+15]&4 != 0 {
					rnis.Sos = true
				} else {
					rnis.Sos = false
				}
				rnis.Speed = binary.LittleEndian.Uint16(message[index+16 : index+18])
				rnis.Bearing = binary.LittleEndian.Uint16(message[index+22 : index+24])
			} else {
				err = errors.New("NavData type 0 is too short")
				return
			}
		case 100:
			if index+3 < MesLen {
				DataLen := binary.LittleEndian.Uint16(message[index+4 : index+6])
				if index+3+int(DataLen) < MesLen {
					index = index + 3 + int(DataLen)
				} else {
					err = errors.New("NavData type 100 is too short")
					return
				}
			} else {
				err = errors.New("NavData type 100 is too short")
				return
			}
		default:
			DataLen, ok := navDataLength[Type]
			if ok {
				if index+DataLen > MesLen {
					index = index + DataLen
				} else {
					err = fmt.Errorf("NavData type %d is too short", Type)
					return
				}
			} else {
				err = fmt.Errorf("Unknown type of NavData: %d", Type)
				return
			}
		}
	}
	return
}
