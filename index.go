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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

// Default index settings.
const (
	InputDefinitionDir = ".input-definitions"
)

// Index represents a container for frames.
type Index struct {
	mu   sync.RWMutex
	path string
	name string

	// Frames by name.
	frames map[string]*Frame

	// Max Slice on any node in the cluster, according to this node.
	remoteMaxSlice        uint64
	remoteMaxInverseSlice uint64

	NewAttrStore func(string) AttrStore

	// Column attribute storage and cache.
	columnAttrStore AttrStore

	// InputDefinitions by name.
	inputDefinitions map[string]*InputDefinition

	broadcaster Broadcaster
	Stats       StatsClient

	Logger Logger
}

// NewIndex returns a new instance of Index.
func NewIndex(path, name string) (*Index, error) {
	err := ValidateName(name)
	if err != nil {
		return nil, err
	}

	return &Index{
		path:             path,
		name:             name,
		frames:           make(map[string]*Frame),
		inputDefinitions: make(map[string]*InputDefinition),

		remoteMaxSlice:        0,
		remoteMaxInverseSlice: 0,

		NewAttrStore:    NewNopAttrStore,
		columnAttrStore: NopAttrStore,

		broadcaster: NopBroadcaster,
		Stats:       NopStatsClient,
		Logger:      NopLogger,
	}, nil
}

// Name returns name of the index.
func (i *Index) Name() string { return i.name }

// Path returns the path the index was initialized with.
func (i *Index) Path() string { return i.path }

// ColumnAttrStore returns the storage for column attributes.
func (i *Index) ColumnAttrStore() AttrStore { return i.columnAttrStore }

// Options returns all options for this index.
func (i *Index) Options() IndexOptions {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.options()
}

func (i *Index) options() IndexOptions {
	return IndexOptions{}
}

// Open opens and initializes the index.
func (i *Index) Open() error {
	// Ensure the path exists.
	if err := os.MkdirAll(i.path, 0777); err != nil {
		return err
	}

	// Read meta file.
	if err := i.loadMeta(); err != nil {
		return err
	}

	if err := i.openFrames(); err != nil {
		return err
	}

	if err := i.columnAttrStore.Open(); err != nil {
		return err
	}

	if err := i.openInputDefinitions(); err != nil {
		return err
	}

	return nil
}

// openFrames opens and initializes the frames inside the index.
func (i *Index) openFrames() error {
	f, err := os.Open(i.path)
	if err != nil {
		return err
	}
	defer f.Close()

	fis, err := f.Readdir(0)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if !fi.IsDir() || fi.Name() == InputDefinitionDir {
			continue
		}

		fr, err := i.newFrame(i.FramePath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
		if err != nil {
			return ErrName
		}
		if err := fr.Open(); err != nil {
			return fmt.Errorf("open frame: name=%s, err=%s", fr.Name(), err)
		}
		i.frames[fr.Name()] = fr
	}
	return nil
}

// loadMeta reads meta data for the index, if any.
func (i *Index) loadMeta() error {
	var pb internal.IndexMeta

	// Read data from meta file.
	buf, err := ioutil.ReadFile(filepath.Join(i.path, ".meta"))
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	} else {
		if err := proto.Unmarshal(buf, &pb); err != nil {
			return err
		}
	}

	// Copy metadata fields.

	return nil
}

// NOTE: Until we introduce new attributes to store in the index .meta file,
// we don't need to actually write the file. The code related to index.options
// and the index meta file are left in place for future use.
/*
// saveMeta writes meta data for the index.
func (i *Index) saveMeta() error {
	// Marshal metadata.
	buf, err := proto.Marshal(&internal.IndexMeta{})
	if err != nil {
		return err
	}

	// Write to meta file.
	if err := ioutil.WriteFile(filepath.Join(i.path, ".meta"), buf, 0666); err != nil {
		return err
	}

	return nil
}
*/

// Close closes the index and its frames.
func (i *Index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Close the attribute store.
	i.columnAttrStore.Close()

	// Close all frames.
	for _, f := range i.frames {
		if err := f.Close(); err != nil {
			return err
		}
	}
	i.frames = make(map[string]*Frame)

	return nil
}

// MaxSlice returns the max slice in the index according to this node.
func (i *Index) MaxSlice() uint64 {
	if i == nil {
		return 0
	}
	i.mu.RLock()
	defer i.mu.RUnlock()

	max := i.remoteMaxSlice
	for _, f := range i.frames {
		if slice := f.MaxSlice(); slice > max {
			max = slice
		}
	}

	i.Stats.Gauge("maxSlice", float64(max), 1.0)
	return max
}

// SetRemoteMaxSlice sets the remote max slice value received from another node.
func (i *Index) SetRemoteMaxSlice(newmax uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.remoteMaxSlice = newmax
}

// MaxInverseSlice returns the max inverse slice in the index according to this node.
func (i *Index) MaxInverseSlice() uint64 {
	if i == nil {
		return 0
	}
	i.mu.RLock()
	defer i.mu.RUnlock()

	max := i.remoteMaxInverseSlice
	for _, f := range i.frames {
		if slice := f.MaxInverseSlice(); slice > max {
			max = slice
		}
	}
	return max
}

// SetRemoteMaxInverseSlice sets the remote max inverse slice value received from another node.
func (i *Index) SetRemoteMaxInverseSlice(v uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.remoteMaxInverseSlice = v
}

// FramePath returns the path to a frame in the index.
func (i *Index) FramePath(name string) string { return filepath.Join(i.path, name) }

// InputDefinitionPath returns the path to the input definition directory for the index.
func (i *Index) InputDefinitionPath() string {
	return filepath.Join(i.path, InputDefinitionDir)
}

// Frame returns a frame in the index by name.
func (i *Index) Frame(name string) *Frame {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.frame(name)
}

// InputDefinition returns an input definition in the index by name.
func (i *Index) InputDefinition(name string) (*InputDefinition, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if inputDef, ok := i.inputDefinitions[name]; ok {
		return inputDef, nil
	}
	return nil, ErrInputDefinitionNotFound
}

func (i *Index) frame(name string) *Frame { return i.frames[name] }

func (i *Index) inputDefinition(name string) *InputDefinition { return i.inputDefinitions[name] }

// Frames returns a list of all frames in the index.
func (i *Index) Frames() []*Frame {
	i.mu.RLock()
	defer i.mu.RUnlock()

	a := make([]*Frame, 0, len(i.frames))
	for _, f := range i.frames {
		a = append(a, f)
	}
	sort.Sort(frameSlice(a))

	return a
}

// InputDefinitions returns a list of all inputDefinitions in the index.
func (i *Index) InputDefinitions() []*InputDefinition {
	i.mu.RLock()
	defer i.mu.RUnlock()

	a := make([]*InputDefinition, 0, len(i.inputDefinitions))
	for _, d := range i.inputDefinitions {
		a = append(a, d)
	}
	//sort.Sort(inputDefintionSlice(a)) // TODO

	return a
}

// RecalculateCaches recalculates caches on every frame in the index.
func (i *Index) RecalculateCaches() {
	for _, frame := range i.Frames() {
		frame.RecalculateCaches()
	}
}

// CreateFrame creates a frame.
func (i *Index) CreateFrame(name string, opt FrameOptions) (*Frame, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Ensure frame doesn't already exist.
	if i.frames[name] != nil {
		return nil, ErrFrameExists
	}
	return i.createFrame(name, opt)
}

// CreateFrameIfNotExists creates a frame with the given options if it doesn't exist.
func (i *Index) CreateFrameIfNotExists(name string, opt FrameOptions) (*Frame, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Find frame in cache first.
	if f := i.frames[name]; f != nil {
		return f, nil
	}

	return i.createFrame(name, opt)
}

func (i *Index) createFrame(name string, opt FrameOptions) (*Frame, error) {
	if name == "" {
		return nil, errors.New("frame name required")
	} else if opt.CacheType != "" && !IsValidCacheType(opt.CacheType) {
		return nil, ErrInvalidCacheType
	}

	// Validate mutually exclusive options if ranges are enabled.
	if opt.RangeEnabled {
		i.Logger.Printf("RangeEnabled is deprecated - no need to set RangeEnabled to true when creating a frame")
	}

	// Validate fields.
	for _, field := range opt.Fields {
		if err := ValidateField(field); err != nil {
			return nil, err
		}
	}

	// Initialize frame.
	f, err := i.newFrame(i.FramePath(name), name)
	if err != nil {
		return nil, err
	}

	// Open frame.
	if err := f.Open(); err != nil {
		return nil, err
	}

	// Set the time quantum.
	if err := f.SetTimeQuantum(opt.TimeQuantum); err != nil {
		f.Close()
		return nil, err
	}

	// Set cache type.
	if opt.CacheType == "" {
		opt.CacheType = DefaultCacheType
	}
	f.cacheType = opt.CacheType

	if opt.CacheSize != 0 {
		f.cacheSize = opt.CacheSize
	}

	f.inverseEnabled = opt.InverseEnabled

	// Set fields.
	f.fields = opt.Fields

	if err := f.saveMeta(); err != nil {
		f.Close()
		return nil, err
	}

	// Add to index's frame lookup.
	i.frames[name] = f

	return f, nil
}

func (i *Index) newFrame(path, name string) (*Frame, error) {
	f, err := NewFrame(path, i.name, name)
	if err != nil {
		return nil, err
	}
	f.Logger = i.Logger
	f.Stats = i.Stats.WithTags(fmt.Sprintf("frame:%s", name))
	f.broadcaster = i.broadcaster
	f.rowAttrStore = i.NewAttrStore(filepath.Join(f.path, ".data"))
	return f, nil
}

// DeleteFrame removes a frame from the index.
func (i *Index) DeleteFrame(name string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Ignore if frame doesn't exist.
	f := i.frame(name)
	if f == nil {
		return nil
	}

	// Close frame.
	if err := f.Close(); err != nil {
		return err
	}

	// Delete frame directory.
	if err := os.RemoveAll(i.FramePath(name)); err != nil {
		return err
	}

	// Remove reference.
	delete(i.frames, name)

	return nil
}

type indexSlice []*Index

func (p indexSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p indexSlice) Len() int           { return len(p) }
func (p indexSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }

// IndexInfo represents schema information for an index.
type IndexInfo struct {
	Name   string       `json:"name"`
	Frames []*FrameInfo `json:"frames"`
}

type indexInfoSlice []*IndexInfo

func (p indexInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p indexInfoSlice) Len() int           { return len(p) }
func (p indexInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }

// EncodeIndexes converts a into its internal representation.
func EncodeIndexes(a []*Index) []*internal.Index {
	other := make([]*internal.Index, len(a))
	for i := range a {
		other[i] = encodeIndex(a[i])
	}
	return other
}

// encodeIndex converts d into its internal representation.
func encodeIndex(d *Index) *internal.Index {
	return &internal.Index{
		Name:             d.name,
		Frames:           encodeFrames(d.Frames()),
		InputDefinitions: encodeInputDefinitions(d.InputDefinitions()),
	}
}

// IndexOptions represents options to set when initializing an index.
type IndexOptions struct{}

// Encode converts i into its internal representation.
func (i *IndexOptions) Encode() *internal.IndexMeta {
	return &internal.IndexMeta{}
}

// hasTime returns true if a contains a non-nil time.
func hasTime(a []*time.Time) bool {
	for _, t := range a {
		if t != nil {
			return true
		}
	}
	return false
}

type importKey struct {
	View  string
	Slice uint64
}

type importData struct {
	RowIDs    []uint64
	ColumnIDs []uint64
}

type importValueData struct {
	ColumnIDs []uint64
	Values    []int64
}

// CreateInputDefinition creates a new input definition.
func (i *Index) CreateInputDefinition(pb *internal.InputDefinition) (*InputDefinition, error) {
	// Ensure input definition doesn't already exist.
	if i.inputDefinitions[pb.Name] != nil {
		return nil, ErrInputDefinitionExists
	}
	return i.createInputDefinition(pb)
}

func (i *Index) createInputDefinition(pb *internal.InputDefinition) (*InputDefinition, error) {
	if pb.Name == "" {
		return nil, ErrInputDefinitionNameRequired
	}

	for _, fr := range pb.Frames {
		opt := FrameOptions{
			// Deprecating row labels per #810. So, setting the default row label here.
			InverseEnabled: fr.Meta.InverseEnabled,
			CacheType:      fr.Meta.CacheType,
			CacheSize:      fr.Meta.CacheSize,
			TimeQuantum:    TimeQuantum(fr.Meta.TimeQuantum),
		}
		_, err := i.CreateFrame(fr.Name, opt)
		if err == ErrFrameExists {
			continue
		} else if err != nil {
			return nil, err
		}
	}

	// Initialize input definition.
	inputDef, err := i.newInputDefinition(pb.Name)
	if err != nil {
		return nil, err
	}

	if err = inputDef.LoadDefinition(pb); err != nil {
		return nil, err
	}
	if err = inputDef.saveMeta(); err != nil {
		return nil, err
	}
	i.inputDefinitions[pb.Name] = inputDef
	return inputDef, nil
}

func (i *Index) newInputDefinition(name string) (*InputDefinition, error) {
	inputDef, err := NewInputDefinition(i.InputDefinitionPath(), i.name, name)
	if err != nil {
		return nil, err
	}
	return inputDef, nil
}

// DeleteInputDefinition removes an input definition from the index.
func (i *Index) DeleteInputDefinition(name string) error {
	// Fail if input definition doesn't exist.
	_, err := i.InputDefinition(name)
	if err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	// Delete input definition file.
	if err := os.Remove(filepath.Join(i.InputDefinitionPath(), name)); err != nil {
		return err
	}

	// Remove reference.
	delete(i.inputDefinitions, name)
	return nil
}

// openInputDefinitions opens and initializes the input definitions inside the index.
func (i *Index) openInputDefinitions() error {
	inputDef, err := os.Open(i.InputDefinitionPath())
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer inputDef.Close()

	inputFiles, err := inputDef.Readdir(0)
	for _, file := range inputFiles {
		input, err := i.newInputDefinition(file.Name())
		if err != nil {
			return err
		}
		input.Open()
		i.inputDefinitions[file.Name()] = input

		// Create frame if it doesn't exist.
		for _, fr := range input.frames {
			_, err := i.CreateFrame(fr.Name, fr.Options)
			if err == ErrFrameExists {
				continue
			} else if err != nil {
				return nil
			}
		}
	}
	return nil
}

// InputBits Process the []Bit though the Frame import process
func (i *Index) InputBits(frame string, bits []*Bit) error {
	var rowIDs, columnIDs []uint64
	var timestamps []*time.Time

	f := i.Frame(frame)
	if f == nil {
		return fmt.Errorf("Frame not found: %s", frame)
	}

	for i, bit := range bits {
		if bit == nil {
			continue
		}
		rowIDs = append(rowIDs, bit.RowID)
		columnIDs = append(columnIDs, bit.ColumnID)

		// Convert timestamps to time.Time.
		if bit.Timestamp > 0 {
			// Don't create a full timestamps slice unless
			// at least one bit contains a timestamp.
			if len(timestamps) == 0 {
				timestamps = make([]*time.Time, len(bits))
			}
			t := time.Unix(bit.Timestamp, 0)
			timestamps[i] = &t
		}
	}

	return f.Import(rowIDs, columnIDs, timestamps)
}
