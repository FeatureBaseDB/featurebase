package roaring

import ( 
	"testing"
)

func TestUnmarshalBinary(t *testing.T) {
	b := NewBitmap()
	confirmedCrashers := []struct {
		cr []byte
	} {
		{cr : []byte(":000000")},
		{cr : []byte("<000000000000000")},
	}

	for _, crash := range confirmedCrashers {
		err := b.UnmarshalBinary(crash.cr)
		if err != nil {
			t.Error("Known crasher failed.")
		}
	}
	
}