package roaring

import (
	"strings"
	"testing"
)

func TestUnmarshalBinary(t *testing.T) {
	b := NewBitmap()
	confirmedCrashers := []struct {
		cr       []byte
		expected string
	}{
		{ // Checks for the zero containers situation
			cr:       []byte(":0\x00\x00\x01\x00\x00\x000000"), //":000000"
			expected: "header: malformed bitmap, key-cardinality slice overruns buffer at 12",
		},
		{ // The next 5 check for malformed bitmaps
			cr: []byte("<0\x0000000000000000000" +
				"\x00\x00\xec\x00\x03\x00\x00\x00\xec000"), //"<000000000000000000ÏÏ000"
			expected: "insufficient data for header + offsets:",
		},
		{
			cr: []byte("<0\x00\x02\x00\x00\x00\\f\x01\xb5\x8d\x009\v\x01\x00\x00\x00\x00" +
				"\x00\x00e\x04\x00\x00\x00\x04\xfd\x00\x01\x00"), //"<0\fµç9e˝"
			expected: "insufficient data for header + offsets:",
		},
		{
			cr:       []byte("<0\x00\x02\x00\x00\x00&x.field safe"), //"<0&x.field safe"
			expected: "insufficient data for header + offsets:",
		},
		{
			cr: []byte("<0\x00\x00\x14\x00\x00\x00\x80\xffp\x05_ 4\x114089" +
				"\x00\x00\xff\x000\x00\x02\x00\x00\x00\x00\xff\u007f\x00\x00\x01\x10\x00\x00j" +
				"\x02\x00\x00$\x04_\x00\xff\u007f\xff062616163\x00" + //"<0Äˇp_ 44089ˇ0ˇj$_ˇˇ0626161630ø¸ad$j√"
				"0\x00\x02\x00\x01\xbf\x00\x04\x00\xfcad$\x00\x00j\x10\x00\x00\xc3"),
			expected: "insufficient data for header + offsets:",
		},
		{ // 0 containers because the container is partially formed, but not fully (ie. 3/12 = 0)
			cr:       []byte("<0\x00\x02\x03\x00\x00\x00쳫\v\x00d9\v\x00\x009\v"), //<0쳫d99
			expected: "insufficient data for header + offsets:",
		},
		{ // Checks for incomplete offset in readWithRuns
			cr:       []byte(";0\x00\x00\v00000"), //";0000000"
			expected: "insufficient data for offsets",
		},
		{ // Checks for incomplete offset in readOffsets
			cr: []byte(":0\x00\x00\x03\x00\x00\x00000000000000" +
				"\x00"), //:0000000000000
			expected: "insufficient data for offsets",
		},
	}

	for _, crash := range confirmedCrashers {
		err := b.UnmarshalBinary(crash.cr)
		if err == nil {
			t.Errorf("expected: %s, got: no error", crash.expected)
		} else if !strings.Contains(err.Error(), crash.expected) {
			t.Errorf("expected: %s, got: %s", crash.expected, err)
		}
	}

}
