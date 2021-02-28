package util

import "encoding/binary"

// PacketStart defines number of byte with metadata in front of binary packet for NDTP server.
// Structure of serialized data is 'PacketStart' bytes with metadata, then binary packet.
const PacketStart = 10

// Data defines deserialized data for NDTP server
type Data struct {
	TerminalID uint32
	SessionID  uint16
	PacketNum  uint32
	Packet     []byte
	ID         []byte
}

// DataEgts defines deserialized data for EGTS server
type DataEgts struct {
	OID       uint32
	PackID    uint16
	RecID     uint16
	SessionID uint64
	Record    []byte
	ID        []byte
}

// Serialize is using for NDTP server serializing data
func Serialize(data Data) []byte {
	bin := make([]byte, PacketStart)
	binary.LittleEndian.PutUint32(bin[:4], data.TerminalID)
	binary.LittleEndian.PutUint16(bin[4:6], data.SessionID)
	binary.LittleEndian.PutUint32(bin[6:], data.PacketNum)
	return append(bin, data.Packet...)
}

// Deserialize is using for NDTP server deserializing data
func Deserialize(bin []byte) Data {
	data := Data{}
	data.TerminalID = binary.LittleEndian.Uint32(bin[0:4])
	//data.SessionID = binary.LittleEndian.Uint16(bin[4:6])
	//data.PacketNum = binary.LittleEndian.Uint32(bin[6:10])
	data.ID = bin[:10]
	data.Packet = bin[PacketStart:]
	return data
}

// TerminalID returns TerminalID from serialized data
func TerminalID(bin []byte) uint32 {
	return binary.LittleEndian.Uint32(bin[0:4])
}
