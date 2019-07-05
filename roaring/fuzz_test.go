// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package roaring

import (
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
			expected: "reading roaring header: malformed bitmap, key-cardinality slice overruns buffer at 12",
		},
		{ // Checks for int overflow
			cr: []byte("<0\x000\x00\x00\x00\x00000000000000" +
				"0"), //"<000000000000000"
			expected: "unmarshaling as pilosa roaring: unknown op type: 48",
		},
		{ // The next 5 check for malformed bitmaps
			cr: []byte("<0\x0000000000000000000" +
				"\x00\x00\xec\x00\x03\x00\x00\x00\xec000"), //"<000000000000000000ÏÏ000"
			expected: "unmarshaling as pilosa roaring: malformed bitmap, key-cardinality not provided for 67372036 containers",
		},
		{
			cr: []byte("<0\x00\x02\x00\x00\x00\\f\x01\xb5\x8d\x009\v\x01\x00\x00\x00\x00" +
				"\x00\x00e\x04\x00\x00\x00\x04\xfd\x00\x01\x00"), //"<0\fµç9e˝"
			expected: "unmarshaling as pilosa roaring: malformed bitmap, key-cardinality not provided for 128625322 containers",
		},
		{
			cr:       []byte("<0\x00\x02\x00\x00\x00&x.field safe"), //"<0&x.field safe"
			expected: "unmarshaling as pilosa roaring: malformed bitmap, key-cardinality not provided for 53127850 containers",
		},
		{
			cr: []byte("<0\x00\x00\x14\x00\x00\x00\x80\xffp\x05_ 4\x114089" +
				"\x00\x00\xff\x000\x00\x02\x00\x00\x00\x00\xff\u007f\x00\x00\x01\x10\x00\x00j" +
				"\x02\x00\x00$\x04_\x00\xff\u007f\xff062616163\x00" + //"<0Äˇp_ 44089ˇ0ˇj$_ˇˇ0626161630ø¸ad$j√"
				"0\x00\x02\x00\x01\xbf\x00\x04\x00\xfcad$\x00\x00j\x10\x00\x00\xc3"),
			expected: "unmarshaling as pilosa roaring: malformed bitmap, key-cardinality not provided for 1 containers",
		},
		{ // 0 containers because the container is partially formed, but not fully (ie. 3/12 = 0)
			cr:       []byte("<0\x00\x02\x03\x00\x00\x00쳫\v\x00d9\v\x00\x009\v"), //<0쳫d99
			expected: "unmarshaling as pilosa roaring: malformed bitmap, key-cardinality not provided for 0 containers",
		},
		{ // Checks for incomplete offset in readWithRuns
			cr:       []byte(";0\x00\x00\v00000"), //";0000000"
			expected: "reading offsets from official roaring format: offset incomplete: len=10",
		},
		{ // Checks for incomplete offset in readOffsets
			cr: []byte(":0\x00\x00\x03\x00\x00\x00000000000000" +
				"\x00"), //:0000000000000
			expected: "reading offsets from official roaring format: offset incomplete: len=1",
		},
	}

	for _, crash := range confirmedCrashers {
		err := b.UnmarshalBinary(crash.cr)
		if err.Error() != crash.expected {
			t.Errorf("Expected: %s, Got: %s", crash.expected, err)
		}
	}

}
