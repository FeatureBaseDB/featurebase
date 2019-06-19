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
			expected : "unmarshaling as pilosa roaring: Maximum operation size exceeded",
		},		
	}

	for _, crash := range confirmedCrashers {
		err := b.UnmarshalBinary(crash.cr)
		if err.Error() != crash.expected {
			t.Errorf("Expected: %s, Got: %s", crash.expected, err)
		} 
	}
	
}