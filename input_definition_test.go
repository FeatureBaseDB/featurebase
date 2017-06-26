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
	def := internal.InputDefinition{Name: "test", Frames: []*internal.Frame{&frames}, Fields: []*internal.InputDefinitionField{&fields}}
	inputDef, err := index.CreateInputDefinition(&def)
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

	internalDef, err := def.Encode()
	if err != nil {
		t.Fatal(err)
	}

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

func TestInputDefinition_LoadDefinition(t *testing.T) {
	index := MustOpenIndex()
	defer index.Close()

	// Create Input Definition.
	input := pilosa.InputDefinition{}
	frames := internal.Frame{Name: "f", Meta: &internal.FrameMeta{RowLabel: "row"}}
	action := internal.InputDefinitionAction{Frame: "f", ValueDestination: "value-to-ROW", ValueMap: map[string]uint64{"Green": 1}}
	field := internal.InputDefinitionField{Name: "id", PrimaryKey: true, InputDefinitionActions: []*internal.InputDefinitionAction{&action}}
	def := &internal.InputDefinition{Name: "test", Frames: []*internal.Frame{&frames}, Fields: []*internal.InputDefinitionField{&field}}
	err := input.LoadDefinition(def)
	if !strings.Contains(err.Error(), "invalid ValueDestination") {
		t.Fatalf("Expected invalid ValueDestination error, actual error: %s", err)
	}

	act := pilosa.Action{Frame: "f", ValueDestination: pilosa.SingleRowBool, ValueMap: map[string]uint64{"Green": 1}}
	_, err = act.Encode()
	if !strings.Contains(err.Error(), "rowID required for single-row-boolean") {
		t.Fatalf("Expected rowID required for single-row-boolean error, actual error: %s", err)
	}

	action = internal.InputDefinitionAction{Frame: "f", ValueDestination: pilosa.Mapping, RowID: 100}
	field = internal.InputDefinitionField{Name: "id", PrimaryKey: true, InputDefinitionActions: []*internal.InputDefinitionAction{&action}}
	def = &internal.InputDefinition{Name: "test", Frames: []*internal.Frame{&frames}, Fields: []*internal.InputDefinitionField{&field}}
	err = input.LoadDefinition(def)
	if !strings.Contains(err.Error(), "valueMap required for map") {
		t.Fatalf("Expected valueMap required for map error, actual error: %s", err)
	}

	action = internal.InputDefinitionAction{Frame: "f", ValueDestination: pilosa.SingleRowBool, RowID: 100}
	action1 := internal.InputDefinitionAction{Frame: "f", ValueDestination: pilosa.SingleRowBool, RowID: 0}
	field1 := internal.InputDefinitionField{Name: "newID", PrimaryKey: true, InputDefinitionActions: []*internal.InputDefinitionAction{&action1}}
	def = &internal.InputDefinition{Name: "test", Frames: []*internal.Frame{&frames}, Fields: []*internal.InputDefinitionField{&field, &field1}}
	err = input.LoadDefinition(def)
	if !strings.Contains(err.Error(), "duplicate primaryKey with other field") {
		t.Fatalf("Expected duplicate primaryKey error, actual error: %s", err)
	}

	action1 = internal.InputDefinitionAction{Frame: "f", ValueDestination: pilosa.SingleRowBool, RowID: 100}
	field1 = internal.InputDefinitionField{Name: "id", PrimaryKey: true, InputDefinitionActions: []*internal.InputDefinitionAction{&action1}}
	def = &internal.InputDefinition{Name: "test", Frames: []*internal.Frame{&frames}, Fields: []*internal.InputDefinitionField{&field, &field1}}
	err = input.LoadDefinition(def)
	if !strings.Contains(err.Error(), "duplicate rowID with other field") {
		t.Fatalf("Expected duplicate rowID with other field error, actual error: %s", err)
	}

	action = internal.InputDefinitionAction{ValueDestination: pilosa.SingleRowBool, RowID: 100}
	def = &internal.InputDefinition{Name: "test", Frames: []*internal.Frame{&frames}, Fields: []*internal.InputDefinitionField{&field}}
	err = input.LoadDefinition(def)
	if !strings.Contains(err.Error(), "frame required") {
		t.Fatalf("Expected frame required error, actual error: %s", err)
	}
}

func TestHandleAction(t *testing.T) {
	var value interface{}
	colID := uint64(0)
	rowID := uint64(100)
	action := pilosa.Action{ValueDestination: pilosa.SingleRowBool, RowID: &rowID}

	value = 1
	b, err := pilosa.HandleAction(&action, value, colID)
	if b != nil {
		t.Fatalf("Expected integer type is not handled by single-row-boolean")
	} else if !strings.Contains(err.Error(), "single-row-boolean value") {
		t.Fatalf("Expected single-row-boolean value error, actual error: %s", err)
	}

	value = "1"
	b, err = pilosa.HandleAction(&action, value, colID)
	if b != nil {
		t.Fatalf("Expected Ignore strings, only accept boolean")
	}

	value = "t"
	b, err = pilosa.HandleAction(&action, value, colID)
	if !strings.Contains(err.Error(), "must equate to a Bool") {
		t.Fatalf("Expected Unrecognized Value Destination error, actual error: %s", err)
	}

	value = float64(1.5)
	b, err = pilosa.HandleAction(&action, value, colID)
	if b != nil {
		if b.RowID != 100 {
			t.Fatalf("Unexpected rowID %v", b.RowID)
		}
	}
	value = float64(0)
	b, err = pilosa.HandleAction(&action, value, colID)
	if b != nil {
		t.Fatalf("Expected Ignore values that do not equate to True")
	}

	value = false
	b, err = pilosa.HandleAction(&action, value, colID)
	if b != nil {
		t.Fatalf("Expected Ignore values that do not equate to True")
	}

	value = true
	b, err = pilosa.HandleAction(&action, value, colID)
	if b != nil {
		if b.ColumnID != 0 {
			t.Fatalf("Unexpected ColumnID %v", b.ColumnID)
		}
	}

	action.ValueDestination = pilosa.ValueToRow
	rowID = 101
	value = float64(25.0)
	b, err = pilosa.HandleAction(&action, value, colID)
	if b != nil {
		if b.RowID != 25 {
			t.Fatalf("Unexpected RowID %v", b.RowID)
		}
	}
	value = "25"
	b, err = pilosa.HandleAction(&action, value, colID)
	if b != nil {
		t.Fatalf("Expected Ignore values that are not type float64")
	}

	action.ValueDestination = pilosa.Mapping
	value = "test"
	b, err = pilosa.HandleAction(&action, value, colID)
	if b != nil {
		t.Fatalf("Expected Ignore values that are not type string")
	}

	value = 25
	b, err = pilosa.HandleAction(&action, value, colID)
	if b != nil {
		t.Fatalf("Expected Ignore values that are not type string")
	}

	action.ValueDestination = "test"
	b, err = pilosa.HandleAction(&action, value, colID)
	if !strings.Contains(err.Error(), "Unrecognized Value Destination") {
		t.Fatalf("Expected Unrecognized Value Destination error, actual error: %s", err)
	}

}
