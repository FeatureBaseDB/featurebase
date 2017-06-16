package pilosa

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

type InputDefinition struct {
	name        string
	path        string
	index       string
	broadcaster Broadcaster
	frames      []InputFrame
	fields      []Field
}

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
func (i *InputDefinition) Fields() []Field { return i.fields }

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

	for _, field := range pb.Fields {
		var actions []Action
		for _, action := range field.Actions {
			actions = append(actions, Action{
				Frame:            action.Frame,
				ValueDestination: action.ValueDestination,
				ValueMap:         action.ValueMap,
				RowID:            action.RowID,
			})
		}

		inputField := Field{
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
		var actions []*internal.Action
		for _, action := range field.Actions {
			actionMeta := &internal.Action{
				Frame:            action.Frame,
				ValueDestination: action.ValueDestination,
				ValueMap:         action.ValueMap,
				RowID:            action.RowID,
			}
			actions = append(actions, actionMeta)
		}

		fieldMeta := &internal.InputDefinitionField{
			Name:       field.Name,
			PrimaryKey: field.PrimaryKey,
			Actions:    actions,
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

// Field descripes a single field mapping in the InputDefinition.
type Field struct {
	Name       string   `json:"name,omitempty"`
	PrimaryKey bool     `json:"primaryKey,omitempty"`
	Actions    []Action `json:"actions,omitempty"`
}

// Encode converts Field into its internal representation.
func (o *Field) Encode() *internal.InputDefinitionField {
	field := internal.InputDefinitionField{Name: o.Name, PrimaryKey: o.PrimaryKey}
	for _, action := range o.Actions {
		field.Actions = append(field.Actions, action.Encode())
	}
	return &field
}

// Action descripes the mapping method for the field in the InputDefinition.
type Action struct {
	Frame            string            `json:"frame,omitempty"`
	ValueDestination string            `json:"valueDestination,omitempty"`
	ValueMap         map[string]uint64 `json:"valueMap,omitempty"`
	RowID            uint64            `json:"rowID,omitempty"`
}

// Encode converts Action into its internal representation.
func (o *Action) Encode() *internal.Action {
	return &internal.Action{
		Frame:            o.Frame,
		ValueDestination: o.ValueDestination,
		ValueMap:         o.ValueMap,
		RowID:            o.RowID,
	}
}

// InputFrame defines the frame used in the input definition.
type InputFrame struct {
	Name    string       `json:"name,omitempty"`
	Options FrameOptions `json:"options,omitempty"`
}

// InputDefinitionInfo the json message format to create an InputDefinition.
type InputDefinitionInfo struct {
	Frames []InputFrame `json:"frames"`
	Fields []Field      `json:"fields"`
}

// Encode converts InputDefinitionInfo into its internal representation.
func (o *InputDefinitionInfo) Encode() *internal.InputDefinition {
	var def internal.InputDefinition
	for _, f := range o.Frames {
		def.Frames = append(def.Frames, &internal.Frame{Name: f.Name, Meta: f.Options.Encode()})
	}
	for _, f := range o.Fields {
		def.Fields = append(def.Fields, f.Encode())
	}

	return &def
}
