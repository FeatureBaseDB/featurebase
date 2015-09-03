package pilosa

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

var counter = uint64(0)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

// SUUID represents a sequential UUID.
type SUUID uint64

// SUUID returns a new SUUID.
func NewSUUID() SUUID {
	millis := uint64(time.Now().UTC().UnixNano())
	id := millis << (64 - 41)
	id |= uint64(rand.Intn(128)) << (64 - 41 - 13)
	id |= counter % 1024
	counter += 1
	return SUUID(id)
}

// String returns a string representation of id.
func (id SUUID) String() string {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(id))
	return hex.EncodeToString(buf[:])
}

// ParseSUUID parses s into an SUUID.
func ParseSUUID(s string) SUUID {
	if n := len(s); n < 16 {
		s = strings.Repeat("0", 16-n) + s
	}

	b, _ := hex.DecodeString(s)
	return SUUID(binary.BigEndian.Uint64(b))
}

// GUID represents a globally unique identifier.
type GUID [16]byte

// UnmarshalText parses a text value into a GUID.
// This is used by the TOML parser.
func (id *GUID) UnmarshalText(text []byte) error {
	v, err := ParseGUID(string(text))
	if err != nil {
		return err
	}

	*id = v
	return nil
}

func (id GUID) String() string {
	var offsets = [...]int{0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34}
	const hexString = "0123456789abcdef"
	r := make([]byte, 36)
	for i, b := range id {
		r[offsets[i]] = hexString[b>>4]
		r[offsets[i]+1] = hexString[b&0xF]
	}
	r[8] = '-'
	r[13] = '-'
	r[18] = '-'
	r[23] = '-'
	return string(r)

}

// Equals returns true if id equals other.
func (id *GUID) Equals(other *GUID) bool {
	for i, v := range id {
		if v != other[i] {
			return false
		}
	}
	return true
}

// NewGUID returns a random GUID.
func NewGUID() GUID {
	uid, _ := gocql.RandomUUID()
	var id GUID
	copy(id[:], uid[:])
	return id
}

// ParseGUID parses s into a GUID.
func ParseGUID(s string) (GUID, error) {
	var u GUID
	j := 0
	for _, r := range s {
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
			return GUID{}, fmt.Errorf("invalid GUID %q", s)
		}
		j += 1
	}
	if j != 32 {
		return GUID{}, fmt.Errorf("invalid GUID %q", s)
	}
	return u, nil
}
