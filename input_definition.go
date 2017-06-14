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

// Name returns the name of input definition was initialized with.
func (i *InputDefinition) Name() string { return i.name }

// Index returns the index name of the input definition was initialized with.
func (i *InputDefinition) Index() string { return i.index }

// Path returns the path of the input definition was initialized with.
func (i *InputDefinition) Path() string { return i.path }

// Frames returns frames of the input definition was initialized with.
func (i *InputDefinition) Frames() []InputFrame { return i.frames }

// Fields returns frames of the input definition was initialized with.
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

func (i *InputDefinition) loadMeta() error {
	var pb internal.InputDefinition
	buf, err := ioutil.ReadFile(filepath.Join(i.path, i.name))
	if err != nil {
		return err
	} else {
		if err := proto.Unmarshal(buf, &pb); err != nil {
			return err
		}
	}
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

type Field struct {
	Name       string   `json:"name,omitempty"`
	PrimaryKey bool     `json:"primaryKey,omitempty"`
	Actions    []Action `json:"actions,omitempty"`
}

type Action struct {
	Frame            string            `json:"frame,omitempty"`
	ValueDestination string            `json:"valueDestination,omitempty"`
	ValueMap         map[string]uint64 `json:"valueMap,omitempty"`
	RowID            uint64            `json:"rowID,omitempty"`
}
