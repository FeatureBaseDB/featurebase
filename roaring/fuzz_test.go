package roaring

import ( 
	"testing"
)

func TestUnmarshalBinary(t *testing.T) {
	b := NewBitmap()
	confirmedCrashers := []struct {
		cr []byte
		expected string
	} {
		{
			cr : []byte(":0\x000\x01\x00\x00\x000000"),					//":000000"
			expected : "reading roaring header: malformed bitmap, key-cardinality slice overruns buffer at 12",
		},				
		{
			cr : []byte("<0\x000\x00\x00\x00\x00000000000000" +
			"0"),														//"<000000000000000"
			expected : "unmarshaling as pilosa roaring: too big",
		},		
	}

	for _, crash := range confirmedCrashers {
		err := b.UnmarshalBinary(crash.cr)
		if err.Error() != crash.expected {
			t.Errorf("Expected: %s, Got: %s", crash.expected, err)
		} 
	}
	
}