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
	"encoding/json"
	"testing"

	"strings"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
)

func TestInputDefinition_Open(t *testing.T) {
	index := MustOpenIndex()
	defer index.Close()

	// Create Input Definition.
	frames := internal.Frame{Name: "f", Meta: &internal.FrameMeta{RowLabel: "row"}}
	action := internal.InputDefinitionAction{Frame: "f", ValueDestination: "mapping", ValueMap: map[string]uint64{"Green": 1}}
	fields := internal.InputDefinitionField{Name: "id", PrimaryKey: true, InputDefinitionActions: []*internal.InputDefinitionAction{&action}}
	def := internal.InputDefinition{Name: "^", Frames: []*internal.Frame{&frames}, Fields: []*internal.InputDefinitionField{&fields}}
	inputDef, err := index.CreateInputDefinition(&def)
	if !strings.Contains(err.Error(), "invalid index or frame's name") {
		t.Fatalf("Expected Invalid name error, actual error: %s", err)
	}

	def = internal.InputDefinition{Name: "test", Frames: []*internal.Frame{&frames}, Fields: []*internal.InputDefinitionField{&fields}}
	inputDef, err = index.CreateInputDefinition(&def)
	if err != nil {
		t.Fatal(err)
	}
	err = inputDef.Open()
	if err != nil {
		t.Fatal(err)
	}
}

// Verify the InputDefinition Encoding to the internal format
func TestInputDefinition_Encoding(t *testing.T) {
	inputBody := []byte(`
			{
			"frames":[{
				"name":"event-time",
				"options":{
					"timeQuantum": "YMD",
					"inverseEnabled": true,
					"cacheType": "ranked"
				}
			}],
			"fields": [
				{
					"name": "id",
					"primaryKey": true
				},
				{
					"name": "cabType",
					"actions": [
						{
							"frame": "cab-type",
							"valueDestination": "mapping",
							"valueMap": {
								"Green": 1,
								"Yellow": 2
							}
						}
					]
				}
			]
		}`)
	var def pilosa.InputDefinitionInfo
	err := json.Unmarshal(inputBody, &def)
	if err != nil {
		t.Fatal(err)
	}

	internalDef := def.Encode()

	if internalDef.Frames[0].Name != "event-time" {
		t.Fatalf("unexpected frame: %v", internalDef)
	} else if internalDef.Frames[0].Meta.CacheType != "ranked" {
		t.Fatalf("unexpected frame meta data: %v", internalDef)
	} else if len(internalDef.Fields) != 2 {
		t.Fatalf("unexpected number of Fields: %d", len(internalDef.Fields))
	} else if len(internalDef.Fields[1].InputDefinitionActions) != 1 {
		t.Fatalf("unexpected number of Actions: %v", internalDef.Fields[1].InputDefinitionActions)
	} else if internalDef.Fields[1].InputDefinitionActions[0].ValueDestination != "mapping" {
		t.Fatalf("unexpected ValueDestination: %v", internalDef.Fields[1].InputDefinitionActions[0])
	}
}

// Test The Action validation cases
func TestActionValidation(t *testing.T) {
	rowID := uint64(100)

	action := pilosa.Action{Frame: "f", ValueDestination: pilosa.InputSingleRowBool, ValueMap: map[string]uint64{"Green": 1}}
	field := pilosa.InputDefinitionField{Name: "id", PrimaryKey: false, Actions: []pilosa.Action{action}}
	info := pilosa.InputDefinitionInfo{Fields: []pilosa.InputDefinitionField{field}}
	err := info.Validate("id")
	if err != pilosa.ErrInputDefinitionAttrsRequired {
		t.Fatalf("Expect error: %s, actual err: %s", pilosa.ErrInputDefinitionAttrsRequired, err)
	}

	frame := pilosa.InputFrame{Name: "f", Options: pilosa.FrameOptions{RowLabel: "row"}}
	info = pilosa.InputDefinitionInfo{Frames: []pilosa.InputFrame{frame}, Fields: []pilosa.InputDefinitionField{field}}
	err = info.Validate("id")
	if !strings.Contains(err.Error(), "rowID required for single-row-boolean") {
		t.Fatalf("Expected rowID required for single-row-boolean error, actual error: %s", err)
	}

	frame = pilosa.InputFrame{Name: "^", Options: pilosa.FrameOptions{RowLabel: "row"}}
	action = pilosa.Action{Frame: "f", ValueDestination: pilosa.InputSingleRowBool, RowID: &rowID}
	field = pilosa.InputDefinitionField{Name: "id", PrimaryKey: true, Actions: []pilosa.Action{action}}
	info = pilosa.InputDefinitionInfo{Frames: []pilosa.InputFrame{frame}, Fields: []pilosa.InputDefinitionField{field}}
	err = info.Validate("id")
	if err != pilosa.ErrName {
		t.Fatalf("Expect error: %s, actual err: %s", pilosa.ErrName, err)
	}

	frame = pilosa.InputFrame{Name: "f", Options: pilosa.FrameOptions{RowLabel: "row"}}
	action = pilosa.Action{ValueDestination: pilosa.InputSingleRowBool, RowID: &rowID}
	field = pilosa.InputDefinitionField{Name: "id", PrimaryKey: true, Actions: []pilosa.Action{action}}
	info = pilosa.InputDefinitionInfo{Frames: []pilosa.InputFrame{frame}, Fields: []pilosa.InputDefinitionField{field}}
	err = info.Validate("id")
	if err != pilosa.ErrFrameRequired {
		t.Fatalf("Expect error: %s, actual err: %s", pilosa.ErrFrameRequired, err)
	}

	action = pilosa.Action{Frame: "f", ValueDestination: pilosa.InputSingleRowBool, RowID: &rowID}
	field = pilosa.InputDefinitionField{Name: "id", PrimaryKey: true, Actions: []pilosa.Action{action}}
	info = pilosa.InputDefinitionInfo{Frames: []pilosa.InputFrame{frame}, Fields: []pilosa.InputDefinitionField{field}}
	err = info.Validate("test")
	if err != pilosa.ErrInputDefinitionColumnLabel {
		t.Fatalf("Expect error: %s, actual err: %s", pilosa.ErrInputDefinitionColumnLabel, err)
	}

	action = pilosa.Action{Frame: "f", ValueDestination: pilosa.InputSingleRowBool, RowID: &rowID}
	field = pilosa.InputDefinitionField{Name: "x", PrimaryKey: false, Actions: []pilosa.Action{action}}
	info = pilosa.InputDefinitionInfo{Frames: []pilosa.InputFrame{frame}, Fields: []pilosa.InputDefinitionField{field}}
	err = info.Validate("id")
	if err != pilosa.ErrInputDefinitionHasPrimaryKey {
		t.Fatalf("Expect error: %s, actual err: %s", pilosa.ErrInputDefinitionHasPrimaryKey, err)
	}

	action = pilosa.Action{Frame: "f", ValueDestination: "value-to-ROW", ValueMap: map[string]uint64{"Green": 1}}
	field = pilosa.InputDefinitionField{Name: "id", PrimaryKey: true, Actions: []pilosa.Action{action}}
	info = pilosa.InputDefinitionInfo{Frames: []pilosa.InputFrame{frame}, Fields: []pilosa.InputDefinitionField{field}}
	err = info.Validate("id")
	if !strings.Contains(err.Error(), "invalid ValueDestination") {
		t.Fatalf("Expected invalid ValueDestination error, actual error: %s", err)
	}

	action = pilosa.Action{Frame: "f", ValueDestination: pilosa.InputMapping, RowID: &rowID}
	field = pilosa.InputDefinitionField{Name: "id", PrimaryKey: true, Actions: []pilosa.Action{action}}
	info = pilosa.InputDefinitionInfo{Frames: []pilosa.InputFrame{frame}, Fields: []pilosa.InputDefinitionField{field}}
	err = info.Validate("id")
	if !strings.Contains(err.Error(), "valueMap required for map") {
		t.Fatalf("Expected valueMap required for map error, actual error: %s", err)
	}

	action = pilosa.Action{Frame: "f", ValueDestination: pilosa.InputSingleRowBool, RowID: &rowID}
	field = pilosa.InputDefinitionField{Name: "test", PrimaryKey: false, Actions: []pilosa.Action{action}}
	action1 := pilosa.Action{Frame: "f", ValueDestination: pilosa.InputSingleRowBool, RowID: &rowID}
	field1 := pilosa.InputDefinitionField{Name: "id", PrimaryKey: true, Actions: []pilosa.Action{action1}}
	info = pilosa.InputDefinitionInfo{Frames: []pilosa.InputFrame{frame}, Fields: []pilosa.InputDefinitionField{field, field1}}
	err = info.Validate("id")
	if !strings.Contains(err.Error(), "duplicate rowID with other field") {
		t.Fatalf("Expected duplicate rowID with other field error, actual error: %s", err)
	}
}

func TestHandleAction(t *testing.T) {
	var value interface{}
	colID := uint64(0)
	rowID := uint64(100)
	action := pilosa.Action{ValueDestination: pilosa.InputSingleRowBool, RowID: &rowID}

	value = 1
	b, err := pilosa.HandleAction(action, value, colID)
	if b != nil {
		t.Fatalf("Expected integer type is not handled by single-row-boolean")
	} else if !strings.Contains(err.Error(), "single-row-boolean value") {
		t.Fatalf("Expected single-row-boolean value error, actual error: %s", err)
	}

	value = "1"
	b, err = pilosa.HandleAction(action, value, colID)
	if b != nil {
		t.Fatalf("Expected Ignore strings, only accept boolean")
	}

	value = "t"
	b, err = pilosa.HandleAction(action, value, colID)
	if !strings.Contains(err.Error(), "must equate to a Bool") {
		t.Fatalf("Expected Unrecognized Value Destination error, actual error: %s", err)
	}

	value = float64(1)
	b, err = pilosa.HandleAction(action, value, colID)
	if !strings.Contains(err.Error(), "must equate to a Bool") {
		t.Fatalf("Expected Unrecognized Value Destination error, actual error: %s", err)
	}

	value = false
	b, err = pilosa.HandleAction(action, value, colID)
	if b != nil {
		t.Fatalf("Expected Ignore values that do not equate to True")
	}

	value = true
	b, err = pilosa.HandleAction(action, value, colID)
	if b != nil {
		if b.ColumnID != 0 {
			t.Fatalf("Unexpected ColumnID %v", b.ColumnID)
		}
		if b.RowID != 100 {
			t.Fatalf("Unexpected rowID %v", b.RowID)
		}
	}

	action.ValueDestination = pilosa.InputValueToRow
	rowID = 101
	value = float64(25.0)
	b, err = pilosa.HandleAction(action, value, colID)
	if b != nil {
		if b.RowID != 25 {
			t.Fatalf("Unexpected RowID %v", b.RowID)
		}
	}
	value = "25"
	b, err = pilosa.HandleAction(action, value, colID)
	if b != nil {
		t.Fatalf("Expected Ignore values that are not type float64")
	}

	action.ValueDestination = pilosa.InputMapping
	value = "test"
	b, err = pilosa.HandleAction(action, value, colID)
	if b != nil {
		t.Fatalf("Expected Ignore values that are not type string")
	}

	value = 25
	b, err = pilosa.HandleAction(action, value, colID)
	if b != nil {
		t.Fatalf("Expected Ignore values that are not type string")
	}

	action.ValueDestination = "test"
	b, err = pilosa.HandleAction(action, value, colID)
	if !strings.Contains(err.Error(), "Unrecognized Value Destination") {
		t.Fatalf("Expected Unrecognized Value Destination error, actual error: %s", err)
	}

}
