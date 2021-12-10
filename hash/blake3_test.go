package hash

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/molecula/featurebase/v2/testhook"
)

func TestBlake3Hasher(t *testing.T) {

	hasher := NewBlake3Hasher()
	hash := make([]byte, 16)
	input := []byte("hello world")
	hash = hasher.CryptoHash(input, hash)
	expected := "d74981efa70a0c880b8d8c1985d075db"
	observed := hex.EncodeToString(hash)
	if observed != expected {
		panic(fmt.Sprintf("expected hash:'%v' but observed hash '%v'", expected, observed))
	}

	obs2 := Blake3sum16(input)
	if obs2 != expected {
		panic(fmt.Sprintf("expected hash:'%v' but observed hash from blake2sum16: '%v'", expected, obs2))
	}
}

func TestCryptoRandInt64(t *testing.T) {
	rnd := CryptoRandInt64()
	if rnd == 0 {
		panic("cryptoRandInt64() gave 0, very high odds it has broken")
	}
}

func TestHashOfDir(t *testing.T) {
	dir, err := testhook.TempDir(t, "TestHashOfDir-dir")
	if err != nil {
		t.Fatal(err)
	}

	b := path.Join(dir, "A", "B")
	if err := os.MkdirAll(b, 0755); err != nil {
		t.Fatal(err)
	}

	c := path.Join(dir, "A", "C")
	if err := os.MkdirAll(c, 0755); err != nil {
		t.Fatal(err)
	}

	bmessage := []byte("hello B\n")
	if err := ioutil.WriteFile(path.Join(b, "b_content"), bmessage, 0644); err != nil {
		t.Fatal(err)
	}

	cmessage := []byte("hello C\n")
	if err := ioutil.WriteFile(path.Join(c, "c_content"), cmessage, 0644); err != nil {
		t.Fatal(err)
	}

	hsh := HashOfDir(dir)

	c2message := []byte("hello C2\n")
	if err := ioutil.WriteFile(path.Join(c, "c_content"), c2message, 0644); err != nil {
		t.Fatal(err)
	}

	hsh2 := HashOfDir(dir)
	if hsh2 == hsh {
		t.Fatal("HashOfDir did not detect 1 byte change")
	}
}
