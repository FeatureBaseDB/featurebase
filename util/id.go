package util

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

var (
	counter = uint64(0)
	Random  *os.File
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
	f, err := os.Open("/dev/urandom")
	if err != nil {
		log.Fatal(err)
	}
	Random = f
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

type GUID [16]byte

func (self GUID) String() string {
	var offsets = [...]int{0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34}
	const hexString = "0123456789abcdef"
	r := make([]byte, 36)
	for i, b := range self {
		r[offsets[i]] = hexString[b>>4]
		r[offsets[i]+1] = hexString[b&0xF]
	}
	r[8] = '-'
	r[13] = '-'
	r[18] = '-'
	r[23] = '-'
	return string(r)

}

func RandomUUID() GUID {
	uid, _ := gocql.RandomUUID()
	var r GUID
	copy(r[:], uid[:])
	return r
}
func ParseGUID(input string) (GUID, error) {
	var u GUID
	j := 0
	for _, r := range input {
		switch {
		case r == '-' && j&1 == 0:
			continue
		case r >= '0' && r <= '9' && j < 32:
			u[j/2] |= byte(r-'0') << uint(4-j&1*4)
		case r >= 'a' && r <= 'f' && j < 32:
			u[j/2] |= byte(r-'a'+10) << uint(4-j&1*4)
		case r >= 'A' && r <= 'F' && j < 32:
			u[j/2] |= byte(r-'A'+10) << uint(4-j&1*4)
		default:
			return GUID{}, fmt.Errorf("invalid GUID %q", input)
		}
		j += 1
	}
	if j != 32 {
		return GUID{}, fmt.Errorf("invalid GUID %q", input)
	}
	return u, nil
}

func In(val int, list []int) bool {
	for _, v := range list {
		if val == v {
			return true
		}
	}
	return false
}

func Difference(a, b []int) []int {
	results := make([]int, 0, len(a))
	for _, v := range a {
		if !In(v, b) {
			results = append(results, v)
		}

	}
	return results
}
