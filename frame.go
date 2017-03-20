package pilosa

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

const (
	// FrameSuffixRank is the suffix used for rank-based frames.
	FrameSuffixRank = ".n"
)

// Default frame settings.
const (
	DefaultRowLabel = "id"
)

// Frame represents a container for views.
type Frame struct {
	mu          sync.Mutex
	path        string
	db          string
	name        string
	timeQuantum TimeQuantum

	views map[string]*View

	// Bitmap attribute storage and cache
	bitmapAttrStore *AttrStore

	stats StatsClient

	// Label used for referring to a row.
	rowLabel string

	LogOutput io.Writer
}

// NewFrame returns a new instance of frame.
func NewFrame(path, db, name string) (*Frame, error) {
	err := ValidateName(name)
	if err != nil {
		return nil, err
	}

	return &Frame{
		path: path,
		db:   db,
		name: name,

		views:           make(map[string]*View),
		bitmapAttrStore: NewAttrStore(filepath.Join(path, ".data")),

		stats: NopStatsClient,

		rowLabel: DefaultRowLabel,

		LogOutput: ioutil.Discard,
	}, nil
}

// Name returns the name the frame was initialized with.
func (f *Frame) Name() string { return f.name }

// DB returns the database name the frame was initialized with.
func (f *Frame) DB() string { return f.db }

// Path returns the path the frame was initialized with.
func (f *Frame) Path() string { return f.path }

// BitmapAttrStore returns the attribute storage.
func (f *Frame) BitmapAttrStore() *AttrStore { return f.bitmapAttrStore }

// MaxSlice returns the max slice in the frame.
func (f *Frame) MaxSlice() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	var max uint64
	for _, view := range f.views {
		if slice := view.MaxSlice(); slice > max {
			max = slice
		}
	}
	return max
}

// SetRowLabel sets the row labels. Persists to meta file on update.
func (f *Frame) SetRowLabel(v string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Ignore if no change occurred.
	if v == "" || f.rowLabel == v {
		return nil
	}

	// Persist meta data to disk on change.
	f.rowLabel = v
	if err := f.saveMeta(); err != nil {
		return err
	}

	return nil
}

// RowLabel returns the row label.
func (f *Frame) RowLabel() string {
	f.mu.Lock()
	v := f.rowLabel
	f.mu.Unlock()
	return v
}

// Options returns all options for this frame.
func (f *Frame) Options() FrameOptions {
	f.mu.Lock()
	opt := FrameOptions{
		RowLabel: f.rowLabel,
	}
	f.mu.Unlock()
	return opt
}

// Open opens and initializes the frame.
func (f *Frame) Open() error {
	if err := func() error {
		// Ensure the frame's path exists.
		if err := os.MkdirAll(f.path, 0777); err != nil {
			return err
		}

		if err := f.loadMeta(); err != nil {
			return err
		}

		if err := f.openViews(); err != nil {
			return err
		}

		if err := f.bitmapAttrStore.Open(); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		f.Close()
		return err
	}

	return nil
}

// openViews opens and initializes the views inside the frame.
func (f *Frame) openViews() error {
	file, err := os.Open(filepath.Join(f.path, "views"))
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer file.Close()

	fis, err := file.Readdir(0)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}

		name := filepath.Base(fi.Name())
		view := f.newView(f.ViewPath(name), name)
		if err := view.Open(); err != nil {
			return fmt.Errorf("open view: view=%s, err=%s", view.Name(), err)
		}
		view.BitmapAttrStore = f.bitmapAttrStore
		f.views[view.Name()] = view

		f.stats.Count("maxSlice", 1)
	}

	return nil
}

// loadMeta reads meta data for the frame, if any.
func (f *Frame) loadMeta() error {
	var pb internal.Frame

	// Read data from meta file.
	buf, err := ioutil.ReadFile(filepath.Join(f.path, ".meta"))
	if os.IsNotExist(err) {
		f.timeQuantum = ""
		f.rowLabel = DefaultRowLabel
		return nil
	} else if err != nil {
		return err
	} else {
		if err := proto.Unmarshal(buf, &pb); err != nil {
			return err
		}
	}

	// Copy metadata fields.
	f.timeQuantum = TimeQuantum(pb.TimeQuantum)
	f.rowLabel = pb.RowLabel

	return nil
}

// saveMeta writes meta data for the frame.
func (f *Frame) saveMeta() error {
	// Marshal metadata.
	buf, err := proto.Marshal(&internal.Frame{
		TimeQuantum: string(f.timeQuantum),
		RowLabel:    f.rowLabel,
	})
	if err != nil {
		return err
	}

	// Write to meta file.
	if err := ioutil.WriteFile(filepath.Join(f.path, ".meta"), buf, 0666); err != nil {
		return err
	}

	return nil
}

// Close closes the frame and its views.
func (f *Frame) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Close the attribute store.
	if f.bitmapAttrStore != nil {
		_ = f.bitmapAttrStore.Close()
	}

	// Close all views.
	for _, view := range f.views {
		_ = view.Close()
	}
	f.views = make(map[string]*View)

	return nil
}

// TimeQuantum returns the time quantum for the frame.
func (f *Frame) TimeQuantum() TimeQuantum {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.timeQuantum
}

// SetTimeQuantum sets the time quantum for the frame.
func (f *Frame) SetTimeQuantum(q TimeQuantum) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Validate input.
	if !q.Valid() {
		return ErrInvalidTimeQuantum
	}

	// Update value on frame.
	f.timeQuantum = q

	// Persist meta data to disk.
	if err := f.saveMeta(); err != nil {
		return err
	}

	return nil
}

// ViewPath returns the path to a view in the frame.
func (f *Frame) ViewPath(name string) string {
	return filepath.Join(f.path, "views", name)
}

// View returns a view in the frame by name.
func (f *Frame) View(name string) *View {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.view(name)
}

func (f *Frame) view(name string) *View { return f.views[name] }

// Views returns a list of all views in the frame.
func (f *Frame) Views() []*View {
	f.mu.Lock()
	defer f.mu.Unlock()

	other := make([]*View, 0, len(f.views))
	for _, view := range f.views {
		other = append(other, view)
	}
	return other
}

func (f *Frame) CreateViewIfNotExists(name string) (*View, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if view := f.views[name]; view != nil {
		return view, nil
	}

	view := f.newView(f.ViewPath(name), name)
	if err := view.Open(); err != nil {
		return nil, err
	}
	view.BitmapAttrStore = f.bitmapAttrStore
	f.views[view.Name()] = view

	return view, nil
}

func (f *Frame) newView(path, name string) *View {
	view := NewView(path, f.db, f.name, name)
	view.LogOutput = f.LogOutput
	view.BitmapAttrStore = f.bitmapAttrStore
	view.stats = f.stats.WithTags(fmt.Sprintf("slice:%s", name))
	return view
}

// SetBit sets a bit within the frame.
func (f *Frame) SetBit(rowID, colID uint64, t *time.Time) (changed bool, err error) {
	// Set standard layout bits.
	if v, err := f.setBit(ViewStandard, rowID, colID, t); err != nil {
		return changed, err
	} else if v {
		changed = v
	}

	// Set inverse layout bits.
	// NOTE: The row & col are transposed for the inverted view.
	if v, err := f.setBit(ViewInverse, colID, rowID, t); err != nil {
		return changed, err
	} else if v {
		changed = v
	}

	return changed, nil
}

// setBit sets a bit for a given layout (default or inverted).
func (f *Frame) setBit(name string, rowID, colID uint64, t *time.Time) (changed bool, err error) {
	// Retrieve view. Exit if it doesn't exist.
	view, err := f.CreateViewIfNotExists(name)
	if err != nil {
		return changed, err
	}

	// Set non-time bit.
	if v, err := view.SetBit(rowID, colID); err != nil {
		return changed, err
	} else if v {
		changed = v
	}

	// Exit early if no timestamp is specified.
	if t == nil {
		return changed, nil
	}

	// If a timestamp is specified then set bits across all views for the quantum.
	for _, subname := range ViewsByTime(name, *t, f.TimeQuantum()) {
		view, err := f.CreateViewIfNotExists(subname)
		if err != nil {
			return changed, err
		}

		if c, err := view.SetBit(rowID, colID); err != nil {
			return changed, err
		} else if c {
			changed = true
		}
	}

	return changed, nil
}

// ClearBit clears a bit within the frame.
func (f *Frame) ClearBit(rowID, colID uint64, t *time.Time) (changed bool, err error) {
	// Clear standard layout bits.
	if v, err := f.clearBit(ViewStandard, rowID, colID, t); err != nil {
		return changed, err
	} else if v {
		changed = v
	}

	// Clear inverse layout bits.
	// NOTE: The row & col are transposed for the inverted view.
	if v, err := f.clearBit(ViewInverse, colID, rowID, t); err != nil {
		return changed, err
	} else if v {
		changed = v
	}

	return changed, nil
}

// clearBit clears a bit for a given layout (default or inverted).
func (f *Frame) clearBit(name string, rowID, colID uint64, t *time.Time) (changed bool, err error) {
	// Retrieve view. Exit if it doesn't exist.
	view, err := f.CreateViewIfNotExists(name)
	if err != nil {
		return changed, err
	}

	// Clear non-time bit.
	if v, err := view.ClearBit(rowID, colID); err != nil {
		return changed, err
	} else if v {
		changed = v
	}

	// Exit early if no timestamp is specified.
	if t == nil {
		return changed, nil
	}

	// If a timestamp is specified then clear bits across all views for the quantum.
	for _, subname := range ViewsByTime(name, *t, f.TimeQuantum()) {
		view, err := f.CreateViewIfNotExists(subname)
		if err != nil {
			return changed, err
		}

		if c, err := view.ClearBit(rowID, colID); err != nil {
			return changed, err
		} else if c {
			changed = true
		}
	}

	return changed, nil
}

// Import bulk imports data.
func (f *Frame) Import(bitmapIDs, profileIDs []uint64, timestamps []*time.Time) error {
	// Determine quantum if timestamps are set.
	q := f.TimeQuantum()
	if hasTime(timestamps) && q == "" {
		return errors.New("time quantum not set in either database or frame")
	}

	// Split import data by fragment.
	dataByFragment := make(map[importKey]importData)
	for i := range bitmapIDs {
		bitmapID, profileID, timestamp := bitmapIDs[i], profileIDs[i], timestamps[i]
		slice := profileID / SliceWidth

		var standard, inverse []string
		if timestamp == nil {
			standard = []string{ViewStandard}
			inverse = []string{ViewInverse}
		} else {
			standard = ViewsByTime(ViewStandard, *timestamp, q)
			inverse = ViewsByTime(ViewInverse, *timestamp, q)
		}

		// Attach bit to each standard view.
		for _, name := range standard {
			key := importKey{View: name, Slice: slice}
			data := dataByFragment[key]
			data.BitmapIDs = append(data.BitmapIDs, bitmapID)
			data.ProfileIDs = append(data.ProfileIDs, profileID)
			dataByFragment[key] = data
		}

		// Attach reversed bits to each inverse view.
		for _, name := range inverse {
			key := importKey{View: name, Slice: slice}
			data := dataByFragment[key]
			data.BitmapIDs = append(data.BitmapIDs, profileID)  // reversed
			data.ProfileIDs = append(data.ProfileIDs, bitmapID) // reversed
			dataByFragment[key] = data
		}
	}

	// Import into each fragment.
	for key, data := range dataByFragment {
		view, err := f.CreateViewIfNotExists(key.View)
		if err != nil {
			return err
		}

		frag, err := view.CreateFragmentIfNotExists(key.Slice)
		if err != nil {
			return err
		}

		if err := frag.Import(data.BitmapIDs, data.ProfileIDs); err != nil {
			return err
		}
	}

	return nil
}

type frameSlice []*Frame

func (p frameSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p frameSlice) Len() int           { return len(p) }
func (p frameSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }

// FrameInfo represents schema information for a frame.
type FrameInfo struct {
	Name  string      `json:"name"`
	Views []*ViewInfo `json:"views,omitempty"`
}

type frameInfoSlice []*FrameInfo

func (p frameInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p frameInfoSlice) Len() int           { return len(p) }
func (p frameInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }

// FrameOptions represents options to set when initializing a frame.
type FrameOptions struct {
	RowLabel string `json:"rowLabel,omitempty"`
}
