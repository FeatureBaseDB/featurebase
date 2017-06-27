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

	"errors"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

// Action Mapping types
const (
	InputMapping       = "mapping"
	InputValueToRow    = "value-to-row"
	InputSingleRowBool = "single-row-boolean"
)

var validValueDestination = []string{InputMapping, InputValueToRow, InputSingleRowBool}

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

// LoadDefinition loads the protobuf format of a defition
func (i *InputDefinition) LoadDefinition(pb *internal.InputDefinition) error {
	// Copy metadata fields.
	i.name = pb.Name
	for _, fr := range pb.Frames {
		frameMeta := fr.Meta
		inputFrame := InputFrame{
			Name: fr.Name,
			Options: FrameOptions{
				RowLabel:       frameMeta.RowLabel,
				InverseEnabled: frameMeta.InverseEnabled,
				CacheSize:      frameMeta.CacheSize,
				CacheType:      frameMeta.CacheType,
				TimeQuantum:    TimeQuantum(frameMeta.TimeQuantum),
			},
		}
		i.frames = append(i.frames, inputFrame)
	}

	numPrimaryKey := 0
	countRowID := make(map[string]uint64)
	for _, field := range pb.Fields {
		var actions []Action
		for _, action := range field.InputDefinitionActions {
			if err := i.ValidateAction(action); err != nil {
				return err
			}
			if action.ValueDestination == InputSingleRowBool && action.Frame != "" {
				val, ok := countRowID[action.Frame]
				if ok && val == action.RowID {
					return fmt.Errorf("duplicate rowID with other field: %v", action.RowID)
				}
				countRowID[action.Frame] = action.RowID
			}
			actions = append(actions, Action{
				Frame:            action.Frame,
				ValueDestination: action.ValueDestination,
				ValueMap:         action.ValueMap,
				RowID:            &action.RowID,
			})
		}
		if field.PrimaryKey {
			numPrimaryKey++
		}

		if numPrimaryKey > 1 {
			return errors.New("duplicate primaryKey with other field")
		}

		inputField := InputDefinitionField{
			Name:       field.Name,
			PrimaryKey: field.PrimaryKey,
			Actions:    actions,
		}
		i.fields = append(i.fields, inputField)
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

//saveMeta writes meta data for the input definition file.
func (i *InputDefinition) saveMeta() error {
	if err := os.MkdirAll(i.path, 0777); err != nil {
		return err
	}
	// Marshal metadata.
	var frames []*internal.Frame
	for _, fr := range i.frames {
		frameMeta := &internal.FrameMeta{
			RowLabel:       fr.Options.RowLabel,
			InverseEnabled: fr.Options.InverseEnabled,
			CacheType:      fr.Options.CacheType,
			CacheSize:      fr.Options.CacheSize,
			TimeQuantum:    string(fr.Options.TimeQuantum),
		}
		frame := &internal.Frame{Name: fr.Name, Meta: frameMeta}
		frames = append(frames, frame)
	}

	var fields []*internal.InputDefinitionField
	for _, field := range i.fields {
		var actions []*internal.InputDefinitionAction
		for _, action := range field.Actions {
			actionMeta := &internal.InputDefinitionAction{
				Frame:            action.Frame,
				ValueDestination: action.ValueDestination,
				ValueMap:         action.ValueMap,
				RowID:            convert(action.RowID),
			}
			actions = append(actions, actionMeta)
		}

		fieldMeta := &internal.InputDefinitionField{
			Name:                   field.Name,
			PrimaryKey:             field.PrimaryKey,
			InputDefinitionActions: actions,
		}
		fields = append(fields, fieldMeta)
	}
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
func (o *InputDefinitionField) Encode() (*internal.InputDefinitionField, error) {
	field := internal.InputDefinitionField{Name: o.Name, PrimaryKey: o.PrimaryKey}

	for _, action := range o.Actions {
		actionEncode, err := action.Encode()
		if err != nil {
			return nil, err
		}
		field.InputDefinitionActions = append(field.InputDefinitionActions, actionEncode)
	}
	return &field, nil
}

// Action describes the mapping method for the field in the InputDefinition.
type Action struct {
	Frame            string            `json:"frame,omitempty"`
	ValueDestination string            `json:"valueDestination,omitempty"`
	ValueMap         map[string]uint64 `json:"valueMap,omitempty"`
	RowID            *uint64           `json:"rowID,omitempty"`
}

// Encode converts Action into its internal representation.
func (o *Action) Encode() (*internal.InputDefinitionAction, error) {
	if o.RowID == nil && o.ValueDestination == "single-row-boolean" {
		return nil, errors.New("rowID required for single-row-boolean")
	}
	return &internal.InputDefinitionAction{
		Frame:            o.Frame,
		ValueDestination: o.ValueDestination,
		ValueMap:         o.ValueMap,
		RowID:            convert(o.RowID),
	}, nil
}

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

// InputDefinitionInfo the json message format to create an InputDefinition.
type InputDefinitionInfo struct {
	Frames []InputFrame           `json:"frames"`
	Fields []InputDefinitionField `json:"fields"`
}

// Encode converts InputDefinitionInfo into its internal representation.
func (i *InputDefinitionInfo) Encode() (*internal.InputDefinition, error) {
	var def internal.InputDefinition
	for _, f := range i.Frames {
		def.Frames = append(def.Frames, &internal.Frame{Name: f.Name, Meta: f.Options.Encode()})
	}
	for _, f := range i.Fields {
		fEncode, err := f.Encode()
		if err != nil {
			return nil, err
		}
		def.Fields = append(def.Fields, fEncode)
	}

	return &def, nil
}

// AddFrame manually add frame to input definition.
func (i *InputDefinition) AddFrame(frame InputFrame) error {
	i.frames = append(i.frames, frame)
	if err := i.saveMeta(); err != nil {
		return err
	}
	return nil
}

// ValidateAction ensures the input definition action conforms to our specification.
func (i *InputDefinition) ValidateAction(action *internal.InputDefinitionAction) error {
	if action.Frame == "" {
		return ErrFrameRequired
	}
	validValues := make(map[string]bool)
	for _, val := range validValueDestination {
		validValues[val] = true
	}
	if _, ok := validValues[action.ValueDestination]; !ok {
		return fmt.Errorf("invalid ValueDestination: %s", action.ValueDestination)
	}
	switch action.ValueDestination {
	case InputMapping:
		if len(action.ValueMap) == 0 {
			return errors.New("valueMap required for map")
		}
	}
	return nil
}

// HandleAction Process the input data with its action and return a bit to be imported later
// Note: if the Bit should not be set then nil is returned with no error
// From the JSON marshalling the possible types are: float64, boolean, string
// TODO handle Timestamps
func HandleAction(a Action, value interface{}, colID uint64) (*Bit, error) {
	var err error
	var bit Bit
	bit.ColumnID = colID

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
	default:
		return nil, fmt.Errorf("Unrecognized Value Destination: %s in Action", a.ValueDestination)
	}
	return &bit, err
}
