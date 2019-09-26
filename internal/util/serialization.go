package util

import "encoding/binary"

const PacketStart = 10

type Data struct {
	TerminalID uint32
	SessionID  uint16
	PacketNum  uint32
	Packet     []byte
	ID         []byte
}

func Serialize(data Data) []byte {
	bin := make([]byte, PacketStart)
	binary.LittleEndian.PutUint32(bin[:4], data.TerminalID)
	binary.LittleEndian.PutUint16(bin[4:6], data.SessionID)
	binary.LittleEndian.PutUint32(bin[6:], data.PacketNum)
	return append(bin, data.Packet...)
}

func Deserialize(bin []byte) Data {
	data := Data{}
	data.TerminalID = binary.LittleEndian.Uint32(bin[0:4])
	//data.SessionID = binary.LittleEndian.Uint16(bin[4:6])
	//data.PacketNum = binary.LittleEndian.Uint32(bin[6:10])
	data.ID = bin[:10]
	data.Packet = bin[PacketStart:]
	return data
}

func TerminalID(bin []byte) uint32 {
	return binary.LittleEndian.Uint32(bin[0:4])
}
