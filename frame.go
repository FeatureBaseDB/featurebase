package pilosa

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	// FrameSuffixTime is the suffix used for time-based frames.
	FrameSuffixTime = ".t"

	// FrameSuffixRank is the suffix used for rank-based frames.
	FrameSuffixRank = ".n"
)

// Frame represents a container for fragments.
type Frame struct {
	mu   sync.Mutex
	path string
	db   string
	name string

	// Fragments by slice.
	fragments map[uint64]*Fragment

	// Bitmap attribute storage and cache
	bitmapAttrStore *AttrStore

	stats StatsClient
}

// NewFrame returns a new instance of frame.
func NewFrame(path, db, name string) *Frame {
	return &Frame{
		path: path,
		db:   db,
		name: name,

		fragments:       make(map[uint64]*Fragment),
		bitmapAttrStore: NewAttrStore(filepath.Join(path, "data")),

		stats: NopStatsClient,
	}
}

// Name returns the name the frame was initialized with.
func (f *Frame) Name() string { return f.name }

// DB returns the database name the frame was initialized with.
func (f *Frame) DB() string { return f.db }

// Path returns the path the frame was initialized with.
func (f *Frame) Path() string { return f.path }

// BitmapAttrStore returns the attribute storage.
func (f *Frame) BitmapAttrStore() *AttrStore { return f.bitmapAttrStore }

// SliceN returns the max slice in the frame.
func (f *Frame) SliceN() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	var max uint64
	for slice := range f.fragments {
		if slice > max {
			max = slice
		}
	}
	return max
}

// Open opens and initializes the frame.
func (f *Frame) Open() error {
	if err := func() error {
		// Ensure the frame's path exists.
		if err := os.MkdirAll(f.path, 0777); err != nil {
			return err
		}

		if err := f.openFragments(); err != nil {
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

// openFragments opens and initializes the fragments inside the frame.
func (f *Frame) openFragments() error {
	file, err := os.Open(f.path)
	if err != nil {
		return err
	}
	defer file.Close()

	fis, err := file.Readdir(0)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}

		// Parse filename into integer.
		slice, err := strconv.ParseUint(filepath.Base(fi.Name()), 10, 64)
		if err != nil {
			continue
		}

		frag := f.newFragment(f.FragmentPath(slice), slice)
		if err := frag.Open(); err != nil {
			return fmt.Errorf("open fragment: slice=%s, err=%s", frag.Slice(), err)
		}
		frag.BitmapAttrStore = f.bitmapAttrStore
		f.fragments[frag.Slice()] = frag

		f.stats.Count("sliceN", 1)
	}

	return nil
}

// Close closes the frame and its fragments.
func (f *Frame) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Close the attribute store.
	if f.bitmapAttrStore != nil {
		_ = f.bitmapAttrStore.Close()
	}

	// Close all fragments.
	for _, frag := range f.fragments {
		_ = frag.Close()
	}
	f.fragments = make(map[uint64]*Fragment)

	return nil
}

// FragmentPath returns the path to a fragment in the frame.
func (f *Frame) FragmentPath(slice uint64) string {
	return filepath.Join(f.path, strconv.FormatUint(slice, 10))
}

// Fragment returns a fragment in the frame by slice.
func (f *Frame) Fragment(slice uint64) *Fragment {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fragment(slice)
}

func (f *Frame) fragment(slice uint64) *Fragment { return f.fragments[slice] }

// Fragments returns a list of all fragments in the frame.
func (f *Frame) Fragments() []*Fragment {
	f.mu.Lock()
	defer f.mu.Unlock()

	other := make([]*Fragment, 0, len(f.fragments))
	for _, fragment := range f.fragments {
		other = append(other, fragment)
	}
	return other
}

// CreateFragmentIfNotExists returns a fragment in the frame by slice.
func (f *Frame) CreateFragmentIfNotExists(slice uint64) (*Fragment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createFragmentIfNotExists(slice)
}

func (f *Frame) createFragmentIfNotExists(slice uint64) (*Fragment, error) {
	// Find fragment in cache first.
	if frag := f.fragments[slice]; frag != nil {
		return frag, nil
	}

	// Initialize and open fragment.
	frag := f.newFragment(f.FragmentPath(slice), slice)
	if err := frag.Open(); err != nil {
		return nil, err
	}
	frag.BitmapAttrStore = f.bitmapAttrStore

	// Save to lookup.
	f.fragments[slice] = frag

	f.stats.Count("sliceN", 1)

	return frag, nil
}

func (f *Frame) newFragment(path string, slice uint64) *Fragment {
	frag := NewFragment(path, f.db, f.name, slice)
	frag.stats = f.stats.WithTags(fmt.Sprintf("slice:%d", slice))
	return frag
}

type frameSlice []*Frame

func (p frameSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p frameSlice) Len() int           { return len(p) }
func (p frameSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }

// FrameInfo represents schema information for a frame.
type FrameInfo struct {
	Name string `json:"name"`
}

type frameInfoSlice []*FrameInfo

func (p frameInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p frameInfoSlice) Len() int           { return len(p) }
func (p frameInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }
