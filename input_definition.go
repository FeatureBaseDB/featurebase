package pilosa

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

type InputDefinition struct {
	name        string
	path        string
	index       string
	broadcaster Broadcaster
	Stats       StatsClient
	LogOutput   io.Writer
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

func (i *InputDefinition) Open() error {
	fmt.Println(i.path)
	if err := func() error {
		// Ensure the input definition's path exists.
		if err := os.MkdirAll(i.path, 0777); err != nil {
			return err
		}

		//if err := i.loadMeta(); err != nil {
		//	return err
		//}

		return nil
	}(); err != nil {
		return err
	}

	return nil
}

//saveMeta writes meta data for the frame.
func (f *InputDefinition) saveMeta() error {
	// Marshal metadata.
	//buf, err := proto.Marshal(&internal.InputDefinitionMeta{
	//	Frames: f.
	//})
	//if err != nil {
	//	return err
	//}

	// Write to meta file.
	if err := ioutil.WriteFile(filepath.Join(f.path, f.name), []byte("123"), 0666); err != nil {
		return err
	}

	return nil
}

// FrameOptions represents options to set when initializing a frame.
type InputDefinitionMeta struct {
	Frames []Frame `json:"frames,omitempty"`
	Fields []Field `json:"fields,omitempty"`
}

type Field struct {
	Name       string   `json:"name,omitempty"`
	PrimaryKey bool     `json:"primaryKey,omitempty"`
	Actions    []Action `json:"action,omitempty"`
}

type Action struct {
	Frame            string            `json:"frame,omitempty"`
	ValueDestination string            `json:"valueDestination,omitempty"`
	ValueMap         map[string]uint64 `json:"valueMap,omitempty"`
	RowID            uint64            `json:"rowID,omitempty"`
}

// Encode converts o into its internal representation.
//func (o *InputDefinitionMeta) Encode() *internal.InputDefinitionMeta {
//	return &internal.InputDefinitionMeta{
//		Frames:                o.Frames,
//		InputDefinitionFields: o.Fields,
//	}
//}
