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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

// Action types.
const (
	InputMapping       = "mapping"
	InputValueToRow    = "value-to-row"
	InputSingleRowBool = "single-row-boolean"
	InputSetTimestamp  = "set-timestamp"
)

var validValueDestination = []string{InputMapping, InputValueToRow, InputSingleRowBool, InputSetTimestamp}

// InputDefinition represents a container for the data input definition.
type InputDefinition struct {
	name        string
	path        string
	index       string
	broadcaster Broadcaster
	frames      []InputFrame
	fields      []InputDefinitionField
}

// NewInputDefinition returns a new instance of InputDefinition.
func NewInputDefinition(path, index, name string) (*InputDefinition, error) {
	err := ValidateName(name)
	if err != nil {
		return nil, err
	}

	return &InputDefinition{
		path:  path,
		index: index,
		name:  name,
	}, nil
}

// Frames returns frames of the input definition was initialized with.
func (i *InputDefinition) Frames() []InputFrame { return i.frames }

// Fields returns fields of the input definition was initialized with.
func (i *InputDefinition) Fields() []InputDefinitionField { return i.fields }

// Open opens and initializes the InputDefinition from file.
func (i *InputDefinition) Open() error {
	if err := func() error {
		if err := os.MkdirAll(i.path, 0777); err != nil {
			return err
		}

		if err := i.loadMeta(); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}
	return nil
}

// LoadDefinition loads the protobuf format of a definition.
func (i *InputDefinition) LoadDefinition(pb *internal.InputDefinition) error {
	// Copy metadata fields.
	i.name = pb.Name
	for _, fr := range pb.Frames {
		frameMeta := fr.Meta
		inputFrame := InputFrame{
			Name: fr.Name,
			Options: FrameOptions{
				// Deprecating row labels per #810. So, setting the default row label here.
				RowLabel:       DefaultRowLabel,
				InverseEnabled: frameMeta.InverseEnabled,
				CacheSize:      frameMeta.CacheSize,
				CacheType:      frameMeta.CacheType,
				TimeQuantum:    TimeQuantum(frameMeta.TimeQuantum),
			},
		}
		i.frames = append(i.frames, inputFrame)
	}

	primaryKeyGiven := false

	for _, field := range pb.Fields {
		var actions []Action
		for _, action := range field.InputDefinitionActions {
			actions = append(actions, Action{
				Frame:            action.Frame,
				ValueDestination: action.ValueDestination,
				ValueMap:         action.ValueMap,
				RowID:            &action.RowID,
			})
		}

		if field.PrimaryKey {
			primaryKeyGiven = true
		}

		inputField := InputDefinitionField{
			Name:       field.Name,
			PrimaryKey: field.PrimaryKey,
			Actions:    actions,
		}
		i.fields = append(i.fields, inputField)
	}

	if len(pb.Fields) > 0 && !primaryKeyGiven {
		return ErrInputDefinitionHasPrimaryKey
	}

	return nil
}

func (i *InputDefinition) loadMeta() error {
	var pb internal.InputDefinition
	buf, err := ioutil.ReadFile(filepath.Join(i.path, i.name))
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}

	return i.LoadDefinition(&pb)
}

// saveMeta writes meta data for the input definition file.
func (i *InputDefinition) saveMeta() error {
	if err := os.MkdirAll(i.path, 0777); err != nil {
		return err
	}

	var frames []*internal.Frame
	for _, fr := range i.frames {
		frames = append(frames, fr.Encode())
	}

	var fields []*internal.InputDefinitionField
	for _, field := range i.fields {
		fields = append(fields, field.Encode())
	}

	// Marshal input definition.
	buf, err := proto.Marshal(&internal.InputDefinition{
		Name:   i.name,
		Frames: frames,
		Fields: fields,
	})
	if err != nil {
		return err
	}

	// Write to meta file.
	if err := ioutil.WriteFile(filepath.Join(i.path, i.name), buf, 0666); err != nil {
		return err
	}

	return nil
}

// InputDefinitionField descripes a single field mapping in the InputDefinition.
type InputDefinitionField struct {
	Name       string   `json:"name,omitempty"`
	PrimaryKey bool     `json:"primaryKey,omitempty"`
	Actions    []Action `json:"actions,omitempty"`
}

// Encode converts InputDefinitionField into its internal representation.
func (o *InputDefinitionField) Encode() *internal.InputDefinitionField {
	var actions []*internal.InputDefinitionAction
	for _, action := range o.Actions {
		actions = append(actions, action.Encode())
	}
	return &internal.InputDefinitionField{
		Name:                   o.Name,
		PrimaryKey:             o.PrimaryKey,
		InputDefinitionActions: actions,
	}
}

// Action describes the mapping method for the field in the InputDefinition.
type Action struct {
	Frame            string            `json:"frame,omitempty"`
	ValueDestination string            `json:"valueDestination,omitempty"`
	ValueMap         map[string]uint64 `json:"valueMap,omitempty"`
	RowID            *uint64           `json:"rowID,omitempty"`
}

// Validate ensures the input definition action conforms to our specification.
func (a *Action) Validate() error {
	if a.Frame == "" {
		return ErrFrameRequired
	}
	if !foundItem(validValueDestination, a.ValueDestination) {
		return fmt.Errorf("invalid ValueDestination: %s", a.ValueDestination)
	}

	switch a.ValueDestination {
	case InputMapping:
		if len(a.ValueMap) == 0 {
			return ErrInputDefinitionValueMap
		}

	}

	return nil
}

// Encode converts Action into its internal representation.
func (a *Action) Encode() *internal.InputDefinitionAction {
	return &internal.InputDefinitionAction{
		Frame:            a.Frame,
		ValueDestination: a.ValueDestination,
		ValueMap:         a.ValueMap,
		RowID:            convert(a.RowID),
	}
}

// convert pointer to uint64.
func convert(x *uint64) uint64 {
	if x != nil {
		return *x
	}
	return 0
}

// InputFrame defines the frame used in the input definition.
type InputFrame struct {
	Name    string       `json:"name,omitempty"`
	Options FrameOptions `json:"options,omitempty"`
}

// Validate the InputFrame data.
func (i *InputFrame) Validate() error {
	if err := ValidateName(i.Name); err != nil {
		return err
	}
	// TODO frame option validation
	return nil
}

// Encode converts InputFrame into its internal representation.
func (i *InputFrame) Encode() *internal.Frame {
	return &internal.Frame{
		Name: i.Name,
		Meta: i.Options.Encode(),
	}
}

// InputDefinitionInfo represents the json message format needed to create an InputDefinition.
type InputDefinitionInfo struct {
	Frames []InputFrame           `json:"frames"`
	Fields []InputDefinitionField `json:"fields"`
}

// Validate the InputDefinitionInfo data.
func (i *InputDefinitionInfo) Validate() error {
	numPrimaryKey := 0
	accountRowID := make(map[string]uint64)

	if len(i.Frames) == 0 || len(i.Fields) == 0 {
		return ErrInputDefinitionAttrsRequired
	}

	for _, frame := range i.Frames {
		if err := frame.Validate(); err != nil {
			return err
		}
	}

	// Validate columnLabel and duplicate primaryKey.
	for _, field := range i.Fields {
		if field.Name == "" {
			return ErrInputDefinitionNameRequired
		}
		for _, action := range field.Actions {
			if err := action.Validate(); err != nil {
				return err
			}
			if action.ValueDestination == InputSingleRowBool {
				if action.RowID == nil {
					return fmt.Errorf("rowID required for single-row-boolean Field %s", field.Name)
				}
				val, ok := accountRowID[action.Frame]
				if ok && val == convert(action.RowID) {
					return fmt.Errorf("duplicate rowID with other field: %v", action.RowID)
				}
				accountRowID[action.Frame] = convert(action.RowID)
			}
		}
		if field.PrimaryKey {
			numPrimaryKey++
		} else if len(field.Actions) == 0 {
			return ErrInputDefinitionActionRequired
		}
	}

	if len(i.Fields) > 0 && numPrimaryKey == 0 {
		return ErrInputDefinitionHasPrimaryKey
	}
	if numPrimaryKey > 1 {
		return ErrInputDefinitionDupePrimaryKey
	}
	return nil
}

// Encode converts InputDefinitionInfo into its internal representation.
func (i *InputDefinitionInfo) Encode() *internal.InputDefinition {
	var def internal.InputDefinition
	for _, f := range i.Frames {
		def.Frames = append(def.Frames, f.Encode())
	}
	for _, f := range i.Fields {
		def.Fields = append(def.Fields, f.Encode())
	}
	return &def
}

// AddFrame manually add frame to input definition.
func (i *InputDefinition) AddFrame(frame InputFrame) error {
	i.frames = append(i.frames, frame)
	if err := i.saveMeta(); err != nil {
		return err
	}
	return nil
}

// HandleAction Process the input data with its action and return a bit to be imported later
// Note: if the Bit should not be set then nil is returned with no error
// From the JSON marshalling the possible types are: float64, boolean, string
func HandleAction(a Action, value interface{}, colID uint64, timestamp int64) (*Bit, error) {
	var err error
	var bit Bit
	bit.ColumnID = colID
	bit.Timestamp = timestamp

	switch a.ValueDestination {
	case InputMapping:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("Mapping value must be a string %v", value)
		}
		bit.RowID, ok = a.ValueMap[v]
		if !ok {
			return nil, fmt.Errorf("Value %s does not exist in definition map", v)
		}
	case InputSingleRowBool:
		v, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("single-row-boolean value %v must equate to a Bool", value)
		}
		if v == false { // False returns a nil error and nil bit.
			return nil, err
		}
		bit.RowID = *a.RowID
	case InputValueToRow:
		v, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("value-to-row value must equate to an integer %v", value)
		}
		bit.RowID = uint64(v)
	case InputSetTimestamp:
		// InputSetTimestamp action is used in the InputJSONDataParser Handler to append a timestamp to all bits in the frame.
		// There are no individual rowID's to set, and the action is a no-op at this step
		return nil, nil
	default:
		return nil, fmt.Errorf("Unrecognized Value Destination: %s in Action", a.ValueDestination)
	}
	return &bit, err
}
