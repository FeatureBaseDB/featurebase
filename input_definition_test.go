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

package pilosa_test

import (
	"github.com/pilosa/pilosa"
	"testing"
)

func TestInputDefinition_Open(t *testing.T) {
	index := MustOpenIndex()
	defer index.Close()

	// Create Input Definition.
	frames := pilosa.InputFrame{Name: "f", Options: pilosa.FrameOptions{RowLabel: "row"}}
	action := pilosa.Action{Frame: "f", ValueDestination: "map", ValueMap: map[string]uint64{"Green": 1}}
	fields := pilosa.Field{Name: "id", PrimaryKey: true, Actions: []pilosa.Action{action}}
	inputDef, err := index.CreateInputDefinition("test", []pilosa.InputFrame{frames}, []pilosa.Field{fields})
	if err != nil {
		t.Fatal(err)
	}
	err = inputDef.Open()
	if err != nil {
		t.Fatal(err)
	}
}
