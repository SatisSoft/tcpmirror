package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

type ndtpData struct {
	NPLType  byte
	NPLReqID uint16
	valid    bool
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
	isResult  bool
	ID        uint32
}

type rnisData struct {
	Time      uint32
	Lon       float64
	Lat       float64
	Bearing   uint16
	Speed     uint16
	Sos       bool
	ID        uint32
	MessageID string
}

func parseNDTP(message []byte) (data ndtpData, packetLen uint16, restBuf []byte, err error) {
	index1 := bytes.Index(message, nplSignature)
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
	} else {
		data.valid = true
	}
	return
}

func parseNPH(message []byte, data *ndtpData) error {
	index := 0
	var nph nphData
	nph.ServiceID = binary.LittleEndian.Uint16(message[index : index+2])
	nph.NPHType = binary.LittleEndian.Uint16(message[index+2 : index+4])
	nph.NPHReqID = binary.LittleEndian.Uint32(message[index+6 : index+10])
	if nph.ServiceID == NPH_SRV_NAVDATA && (nph.NPHType == NPH_SND_HISTORY || nph.NPHType == NPH_SND_REALTIME) {
		rnis, NPHLen, err := parseNavData(message[index+NPH_HEADER_LEN:])
		if err != nil {
			return err
		} else {
			data.ToRnis = rnis
			index = index + +NPHLen
		}
	} else {
		if nph.ServiceID == NPH_SRV_GENERIC_CONTROLS && nph.NPHType == NPH_SGC_CONN_REQUEST {
			nph.ID = binary.LittleEndian.Uint32(message[index+NPH_HEADER_LEN+6 : index+NPH_HEADER_LEN+10])
		} else if nph.NPHType == NPH_RESULT {
			nph.isResult = true
			nph.NPHResult = binary.LittleEndian.Uint32(message[index+NPH_HEADER_LEN : index+NPH_HEADER_LEN+4])
		}
	}
	data.NPH = nph
	return nil
}

func parseNavData(message []byte) (rnis rnisData, index int, err error) {
	MesLen := len(message)
	for index < MesLen-2 {
		switch Type := message[0]; Type {
		case 0:
			DataLen := navDataLength[0]
			if index+DataLen < MesLen {
				var latHS, lonHS int
				rnis.Time = binary.LittleEndian.Uint32(message[index+2 : index+6])
				lon := binary.LittleEndian.Uint32(message[index+6 : index+10])
				lat := binary.LittleEndian.Uint32(message[index+10 : index+14])
				if message[index+15]&32 != 0 {
					latHS = 1
				}
				if message[index+15]&64 != 0 {
					lonHS = 1
				}
				rnis.Lon = float64((2*latHS-1)*int(lat)) / 10000000.0
				rnis.Lat = float64((2*lonHS-1)*int(lon)) / 10000000.0
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

func changePacket(b []byte, data ndtpData, s *session) (uint32, []byte) {
	NPLReqID, NPHReqID := serverID(s)
	NPLReqID1 := new(bytes.Buffer)
	binary.Write(NPLReqID1, binary.LittleEndian, NPLReqID)
	copy(b[13:], NPLReqID1.Bytes())
	NPHReqID1 := new(bytes.Buffer)
	binary.Write(NPHReqID1, binary.LittleEndian, NPHReqID)
	copy(b[NPL_HEADER_LEN+6:], NPHReqID1.Bytes())
	if data.NPH.ServiceID == NPH_SRV_NAVDATA && data.NPH.NPHType == NPH_SND_REALTIME {
		Now := getMill()
		if (Now - int64(data.ToRnis.Time)) > 60000 {
			NPHType1 := new(bytes.Buffer)
			binary.Write(NPHType1, binary.LittleEndian, uint16(NPH_SND_HISTORY))
			copy(b[NPL_HEADER_LEN+2:], NPHType1.Bytes())
		}
	}
	crc := crc16(b[NPL_HEADER_LEN:])
	crc1 := new(bytes.Buffer)
	binary.Write(crc1, binary.LittleEndian, crc)
	copy(b[6:], crc1.Bytes())
	return NPHReqID, b
}

func changePacketFromServ(b []byte, s *session) []byte {
	NPLReqID, NPHReqID := clientID(s)
	NPLReqID1 := new(bytes.Buffer)
	binary.Write(NPLReqID1, binary.LittleEndian, NPLReqID)
	copy(b[13:], NPLReqID1.Bytes())
	NPHReqID1 := new(bytes.Buffer)
	binary.Write(NPHReqID1, binary.LittleEndian, NPHReqID)
	copy(b[NPL_HEADER_LEN+6:], NPHReqID1.Bytes())
	crc := crc16(b[NPL_HEADER_LEN:])
	crc1 := new(bytes.Buffer)
	binary.Write(crc1, binary.LittleEndian, crc)
	copy(b[6:], crc1.Bytes())
	return b
}

func errorAnswer(packet []byte) []byte {
	nph := append(packet[NPL_HEADER_LEN:NPL_HEADER_LEN+NPH_HEADER_LEN], errResult...)
	copy(nph[2:], nphResultType)
	crc := crc16(nph)
	ans := packet[:NPL_HEADER_LEN]
	dataSize := new(bytes.Buffer)
	binary.Write(dataSize, binary.LittleEndian, uint16(NPH_HEADER_LEN+4))
	copy(ans[2:], dataSize.Bytes())
	crc1 := new(bytes.Buffer)
	binary.Write(crc1, binary.LittleEndian, crc)
	copy(ans[6:], crc1.Bytes())
	return ans
}

func changeAddress(data []byte, ip net.IP) {
	start := []byte{0x7E, 0x7E}
	index1 := bytes.Index(data, start)
	for i, j := index1+9, 0; i < index1+13; i, j = i+1, j+1 {
		data[i] = ip[j]
	}
}

func getIP(c net.Conn) net.IP {
	ipPort := strings.Split(c.RemoteAddr().String(), ":")
	ip := ipPort[0]
	ip1 := net.ParseIP(ip)
	return ip1.To4()
}

func answer(packet []byte) []byte {
	nph := append(packet[NPL_HEADER_LEN:NPL_HEADER_LEN+NPH_HEADER_LEN], okResult...)
	copy(nph[2:], nphResultType)
	crc := crc16(nph)
	ans := packet[:NPL_HEADER_LEN]
	dataSize := new(bytes.Buffer)
	binary.Write(dataSize, binary.LittleEndian, uint16(NPH_HEADER_LEN+4))
	copy(ans[2:], dataSize.Bytes())
	crc1 := new(bytes.Buffer)
	binary.Write(crc1, binary.LittleEndian, crc)
	copy(ans[6:], crc1.Bytes())
	return ans
}

func getMill() int64 {
	return time.Now().UnixNano() / 1000000
}
