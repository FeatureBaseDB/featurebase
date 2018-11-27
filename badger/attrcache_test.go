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

package badger

import (
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"
)

func Test_attrCache_Attrs(t *testing.T) {
	log.SetFlags(log.Lshortfile)
	name := "get set attr"
	path, err := ioutil.TempDir("", "get-set-attr-cache")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(path) // clean up

	t.Run(name, func(t *testing.T) {
		s := NewAttrStore(path)
		err := s.Open()
		defer s.Close()
		if err != nil {
			t.Fatalf("attrCache.Open() returned an error %v", err)
		}
		m := map[string]interface{}{
			"some": "value",
		}
		id := uint64(32)
		err = s.SetAttrs(id, m)
		if err != nil {
			t.Fatalf("attrCache.SetAttrs() err: %+v", err)
		}
		err = s.SetAttrs(id, m)
		if err != nil {
			t.Fatalf("again attrCache.SetAttrs() err: %+v", err)
		}
		m2, err := s.Attrs(id)
		if err != nil {
			t.Fatalf("attrCache.Attrs() err: %+v", err)
		}
		if !reflect.DeepEqual(m, m2) {
			t.Fatalf("Test_attrCache_GetSetAttrs maps are different want: \n%+v	got: \n%+v", m, m2)
		}
	})
}

func Test_attrCache_SetGetBulkAttrs(t *testing.T) {
	name := "get set bulk attr"

	path, err := ioutil.TempDir("", "get-set-bulk-attr-cache")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path) // clean up

	t.Run(name, func(t *testing.T) {
		s := NewAttrStore(path)
		err := s.Open()
		defer s.Close()
		if err != nil {
			t.Fatalf("attrCache.Open() returned an error %v", err)
		}
		m := map[uint64]map[string]interface{}{
			1: { //id 1 will be stored in block data 0
				"ten": uint64(10), //ten will return as int64
			},
			102: { //id 102 will be stored in block data 1
				"eleven": int(11), //eleven will return as int64
			},
			202: { //id 202 will be stored in block data 2
				"nil": nil, //when there is one nil value we still store an empty map
			},
		}
		err = s.SetBulkAttrs(m)
		if err != nil {
			t.Fatalf("attrCache.SetBulkAttrs() error = %v", err)
		}
		m2, err := s.BlockData(0)
		if err != nil {
			t.Fatalf("attrCache.BlockData() error = %v", err)
		}
		if v := m2[1]["ten"]; v.(int64) != 10 {
			t.Fatalf("want \n%v, got \n%v", m, m2)
		}
		m3, err := s.BlockData(1)
		if v := m3[102]["eleven"]; v.(int64) != 11 {
			t.Fatalf("want \n%v, got \n%v", m, m3)
		}
		if err != nil {
			t.Fatalf("attrCache.BlockData() error = %v", err)
		}
		m4, err := s.BlockData(2)
		if v := m4[202]["nil"]; v != nil {
			t.Fatalf("want \n%v, got \n%v", m, m4)
		}
		if err != nil {
			t.Fatalf("attrCache.BlockData() error = %v", err)
		}
	})

}
