package util

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"strings"
	"time"
)

var (
	counter = uint64(0)
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

type SUUID uint64

func leftPad(s string, padStr string, pLen int) string {
	return strings.Repeat(padStr, pLen) + s
}
func Id() SUUID {
	millis := uint64(time.Now().UTC().UnixNano())
	id := millis << (64 - 41)
	id |= uint64(rand.Intn(128)) << (64 - 41 - 13)
	id |= counter % 1024
	counter += 1
	return SUUID(id)
}

func SUUID_to_Hex(a SUUID) string {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, a)
	return hex.EncodeToString(buf.Bytes())
}
func Hex_to_SUUID(str string) SUUID {
	l := len(str)
	var m string
	if l < 16 {
		m = leftPad(str, "0", 16-l)
	} else {
		m = str
	}

	b, _ := hex.DecodeString(m)
	num := binary.BigEndian.Uint64(b)
	return SUUID(num)
}
