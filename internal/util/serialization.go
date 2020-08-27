package util

import "encoding/binary"

// PacketStart defines number of byte with metadata in front of binary packet for NDTP server.
// Structure of serialized data is 'PacketStart' bytes with metadata, then binary packet.
const PacketStart = 10

// PacketStartEgts defines number of byte with metadata in front of binary packet for EGTS server.
// Structure of serialized data is 'PacketStart' bytes with metadata, then binary packet.
const PacketStartEgts = 16

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

// Serialize4Egts is using for EGTS server serializing data
func Serialize4Egts(data DataEgts) []byte {
	bin := make([]byte, PacketStartEgts)
	binary.LittleEndian.PutUint32(bin[:4], data.OID)
	binary.LittleEndian.PutUint64(bin[4:12], data.SessionID)
	binary.LittleEndian.PutUint16(bin[12:14], data.PackID)
	binary.LittleEndian.PutUint16(bin[14:], data.RecID)
	return append(bin, data.Record...)
}

// Deserialize4Egts is using for EGTS server deserializing data
func Deserialize4Egts(bin []byte) DataEgts {
	data := DataEgts{}
	data.OID = binary.LittleEndian.Uint32(bin[:4])
	data.ID = bin[:PacketStartEgts]
	data.Record = bin[PacketStartEgts:]
	return data
}

// TerminalID returns TerminalID from serialized data
func TerminalID(bin []byte) uint32 {
	return binary.LittleEndian.Uint32(bin[0:4])
}
