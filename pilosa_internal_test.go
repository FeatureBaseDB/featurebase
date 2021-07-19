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

package pilosa

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/molecula/featurebase/v2/roaring"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
)

func TestValidateName(t *testing.T) {
	names := []string{
		"a", "ab", "ab1", "b-c", "d_e", "exists",
		"longbutnottoolongaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa12345689012345689012345678901234567890",
	}
	for _, name := range names {
		if ValidateName(name) != nil {
			t.Fatalf("Should be valid index name: %s", name)
		}
	}
}

func TestValidateNameInvalid(t *testing.T) {
	names := []string{
		"", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce", "1", "_", "-",
		"long123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1", "_exists",
	}
	for _, name := range names {
		if ValidateName(name) == nil {
			t.Fatalf("Should be invalid index name: %s", name)
		}
	}
}

func TestAPI_CombineForExistence(t *testing.T) {
	bm := roaring.NewBitmap(pos(1, 1), pos(1, 2), pos(1, 3), pos(1, 65537), pos(1, 65538), pos(2, 1), pos(2, 2), pos(2, 5), pos(2, 65537), pos(2, 65538))
	buf := new(bytes.Buffer)
	_, err := bm.WriteTo(buf)
	PanicOn(err)
	raw := buf.Bytes()
	results, err := combineForExistence(raw)
	if err != nil {
		t.Fatalf("failure to combine: %v", err)
	}
	bm2 := roaring.NewBitmap()
	_, _, err = bm2.ImportRoaringBits(results, false, false, 1<<shardVsContainerExponent)
	PanicOn(err)
	expected := []uint64{1, 2, 3, 5, 65537, 65538}
	got := bm2.Slice()
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected:%v got:%v", expected, got)
	}

}
