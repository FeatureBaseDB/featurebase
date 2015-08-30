package util

import (
	"bytes"
	"encoding/binary"
)

const TimeOut = 30

func Int64ToByte(data int64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, data)
	return buf.Bytes()
}
func Uint64ToByte(data uint64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, data)
	return buf.Bytes()
}
func ByteToUint64(data []byte) uint64 {
	var value uint64
	buf := bytes.NewReader(data)
	binary.Read(buf, binary.BigEndian, &value)
	return value
}
func ByteToInt64(data []byte) int64 {
	var value int64
	buf := bytes.NewReader(data)
	binary.Read(buf, binary.BigEndian, &value)
	return value
}
