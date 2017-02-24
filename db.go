package pilosa

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

// DB represents a container for frames.
type DB struct {
	mu   sync.Mutex
	path string
	name string

	// Default time quantum for all frames in database.
	// This can be overridden by individual frames.
	timeQuantum TimeQuantum

	// Frames by name.
	frames map[string]*Frame

	// Max Slice on any node in the cluster, according to this node
	remoteMaxSlice uint64

	// Profile attribute storage and cache
	profileAttrStore *AttrStore

	stats StatsClient

	LogOutput io.Writer
}

// NewDB returns a new instance of DB.
func NewDB(path, name string) *DB {
	return &DB{
		path:           path,
		name:           name,
		frames:         make(map[string]*Frame),
		remoteMaxSlice: 0,

		profileAttrStore: NewAttrStore(filepath.Join(path, "data")),

		stats:     NopStatsClient,
		LogOutput: ioutil.Discard,
	}
}

// Name returns name of the database.
func (db *DB) Name() string { return db.name }

// Path returns the path the database was initialized with.
func (db *DB) Path() string { return db.path }

// ProfileAttrStore returns the storage for profile attributes.
func (db *DB) ProfileAttrStore() *AttrStore { return db.profileAttrStore }

// Open opens and initializes the database.
func (db *DB) Open() error {
	// Ensure the path exists.
	if err := os.MkdirAll(db.path, 0777); err != nil {
		return err
	}

	// Read meta file.
	if err := db.loadMeta(); err != nil {
		return err
	}

	if err := db.openFrames(); err != nil {
		return err
	}

	if err := db.profileAttrStore.Open(); err != nil {
		return err
	}

	return nil
}

// openFrames opens and initializes the frames inside the database.
func (db *DB) openFrames() error {
	f, err := os.Open(db.path)
	if err != nil {
		return err
	}
	defer f.Close()

	fis, err := f.Readdir(0)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}

		fr := db.newFrame(db.FramePath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
		if err := fr.Open(); err != nil {
			return fmt.Errorf("open frame: name=%s, err=%s", fr.Name(), err)
		}
		db.frames[fr.Name()] = fr

		db.stats.Count("frameN", 1)
	}
	return nil
}

// loadMeta reads meta data for the database, if any.
func (db *DB) loadMeta() error {
	var pb internal.DB

	// Read data from meta file.
	buf, err := ioutil.ReadFile(filepath.Join(db.path, "meta"))
	if os.IsNotExist(err) {
		db.timeQuantum = ""
		return nil
	} else if err != nil {
		return err
	} else {
		if err := proto.Unmarshal(buf, &pb); err != nil {
			return err
		}
	}

	// Copy metadata fields.
	db.timeQuantum = TimeQuantum(pb.TimeQuantum)

	return nil
}

// saveMeta writes meta data for the database.
func (db *DB) saveMeta() error {
	// Marshal metadata.
	buf, err := proto.Marshal(&internal.DB{TimeQuantum: string(db.timeQuantum)})
	if err != nil {
		return err
	}

	// Write to meta file.
	if err := ioutil.WriteFile(filepath.Join(db.path, "meta"), buf, 0666); err != nil {
		return err
	}

	return nil
}

// Close closes the database and its frames.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Close the attribute store.
	if db.profileAttrStore != nil {
		db.profileAttrStore.Close()
	}

	// Close all frames.
	for _, f := range db.frames {
		f.Close()
	}
	db.frames = make(map[string]*Frame)

	return nil
}

// MaxSlice returns the max slice in the database according to this node.
func (db *DB) MaxSlice() uint64 {
	if db == nil {
		return 0
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	max := db.remoteMaxSlice
	for _, f := range db.frames {
		if slice := f.MaxSlice(); slice > max {
			max = slice
		}
	}
	return max
}

// TimeQuantum returns the default time quantum for the database.
func (db *DB) TimeQuantum() TimeQuantum {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.timeQuantum
}

// SetTimeQuantum sets the default time quantum for the database.
func (db *DB) SetTimeQuantum(q TimeQuantum) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Validate input.
	if !q.Valid() {
		return ErrInvalidTimeQuantum
	}

	// Update value on database.
	db.timeQuantum = q

	// Perist meta data to disk.
	if err := db.saveMeta(); err != nil {
		return err
	}

	return nil
}

// FramePath returns the path to a frame in the database.
func (db *DB) FramePath(name string) string { return filepath.Join(db.path, name) }

// Frame returns a frame in the database by name.
func (db *DB) Frame(name string) *Frame {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.frame(name)
}

func (db *DB) frame(name string) *Frame { return db.frames[name] }

// Frames returns a list of all frames in the database.
func (db *DB) Frames() []*Frame {
	db.mu.Lock()
	defer db.mu.Unlock()

	a := make([]*Frame, 0, len(db.frames))
	for _, f := range db.frames {
		a = append(a, f)
	}
	sort.Sort(frameSlice(a))

	return a
}

// CreateFrameIfNotExists returns a frame in the database by name.
func (db *DB) CreateFrameIfNotExists(name string) (*Frame, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.createFrameIfNotExists(name)
}

func (db *DB) createFrameIfNotExists(name string) (*Frame, error) {
	if name == "" {
		return nil, errors.New("frame name required")
	}

	// Find frame in cache first.
	if f := db.frames[name]; f != nil {
		return f, nil
	}

	// Initialize and open frame.
	f := db.newFrame(db.FramePath(name), name)
	if err := f.Open(); err != nil {
		return nil, err
	}
	db.frames[name] = f

	db.stats.Count("frameN", 1)

	return f, nil
}

func (db *DB) newFrame(path, name string) *Frame {
	f := NewFrame(path, db.name, name)
	f.LogOutput = db.LogOutput
	f.stats = db.stats.WithTags(fmt.Sprintf("frame:%s", name))
	return f
}

// DeleteFrame removes a frame from the database.
func (db *DB) DeleteFrame(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Ignore if frame doesn't exist.
	f := db.frame(name)
	if f == nil {
		return nil
	}

	// Close frame.
	if err := f.Close(); err != nil {
		return err
	}

	// Delete frame directory.
	if err := os.RemoveAll(db.FramePath(name)); err != nil {
		return err
	}

	// Remove reference.
	delete(db.frames, name)

	db.stats.Count("frameN", -1)

	return nil
}

// CreateFragmentIfNotExists returns a fragment in the database by name/slice.
func (db *DB) CreateFragmentIfNotExists(name string, slice uint64) (*Fragment, error) {
	f, err := db.CreateFrameIfNotExists(name)
	if err != nil {
		return nil, err
	}
	return f.CreateFragmentIfNotExists(slice)
}

// SetBit sets a bit for a given profile & bitmap.
// If a timestamp is specified then set all bits for the different quantum units.
func (db *DB) SetBit(name string, bitmapID, profileID uint64, t *time.Time) (changed bool, err error) {
	// Ensure base frame exists first so we can retrieve quantum info.
	f, err := db.CreateFrameIfNotExists(name)
	if err != nil {
		return changed, err
	}

	// Determine quantum of frame. Set to the default quantum if it is unset.
	var q TimeQuantum
	if t != nil {
		q = f.TimeQuantum()
		if q == "" {
			q = db.TimeQuantum()
			if err := f.SetTimeQuantum(q); err != nil {
				return changed, err
			}
		}
	}

	// Set bit & time bits on the base frame first.
	if v, err := db.setBit(name, bitmapID, profileID, t, q); err != nil {
		return changed, err
	} else if v {
		changed = true
	}

	// Set inverse bit & time bits on the inverted frame next.
	// This swaps the bitmap and profile ids and inserts into a "::I" suffixed frame.
	if v, err := db.setBit(name+InvertedFrameSuffix, profileID, bitmapID, t, q); err != nil {
		return changed, err
	} else if v {
		changed = true
	}

	return changed, nil
}

// setBit sets a bit for a given profile & bitmap.
func (db *DB) setBit(name string, bitmapID, profileID uint64, t *time.Time, q TimeQuantum) (changed bool, err error) {
	// Read frame.
	f, err := db.CreateFrameIfNotExists(name)
	if err != nil {
		return changed, err
	}

	// Set bit and update changed flag.
	if v, err := f.SetBit(bitmapID, profileID); err != nil {
		return changed, err
	} else if v {
		changed = true
	}

	// If this is a non-time bit then simply set the bit on the frame.
	if t == nil {
		return changed, nil
	}

	// If a timestamp is specified then set bits across all frames for the quantum.
	for _, subname := range FramesByTime(name, *t, q) {
		f, err := db.CreateFrameIfNotExists(subname)
		if err != nil {
			return changed, err
		}

		if c, err := f.SetBit(bitmapID, profileID); err != nil {
			return changed, err
		} else if c {
			changed = true
		}
	}
	return changed, nil
}

// Import bulk imports data.
func (db *DB) Import(name string, bitmapIDs, profileIDs []uint64, timestamps []*time.Time) error {
	// Read frame.
	f, err := db.CreateFrameIfNotExists(name)
	if err != nil {
		return err
	}

	// Determine quantum if timestamps are set.
	var q TimeQuantum
	if hasTime(timestamps) {
		if q = f.TimeQuantum(); q == "" {
			q = db.TimeQuantum()
			if err := f.SetTimeQuantum(q); err != nil {
				return err
			}
		}

		if q == "" {
			return errors.New("time quantum not set in either database or frame")
		}
	}

	// Split import data by fragment.
	dataByFragment := make(map[importKey]importData)
	for i := range bitmapIDs {
		bitmapID, profileID, timestamp := bitmapIDs[i], profileIDs[i], timestamps[i]
		slice := profileID / SliceWidth

		var names []string
		if timestamp == nil {
			names = []string{name}
		} else {
			names = FramesByTime(name, *timestamp, q)
		}

		// Attach bit to each frame.
		for _, name := range names {
			key := importKey{Frame: name, Slice: slice}
			data := dataByFragment[key]
			data.BitmapIDs = append(data.BitmapIDs, bitmapID)
			data.ProfileIDs = append(data.ProfileIDs, profileID)
			dataByFragment[key] = data
		}
	}

	// Import into each fragment.
	for key, data := range dataByFragment {
		f, err := db.CreateFragmentIfNotExists(key.Frame, key.Slice)
		if err != nil {
			return err
		}

		if err := f.Import(data.BitmapIDs, data.ProfileIDs); err != nil {
			return err
		}
	}

	return nil
}

type dbSlice []*DB

func (p dbSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p dbSlice) Len() int           { return len(p) }
func (p dbSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }

// DBInfo represents schema information for a database.
type DBInfo struct {
	Name   string       `json:"name"`
	Frames []*FrameInfo `json:"frames"`
}

type dbInfoSlice []*DBInfo

func (p dbInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p dbInfoSlice) Len() int           { return len(p) }
func (p dbInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }

// MergeSchemas combines databases and frames from a and b into one schema.
func MergeSchemas(a, b []*DBInfo) []*DBInfo {
	// Generate a map from both schemas.
	m := make(map[string]map[string]struct{})
	for _, dbs := range [][]*DBInfo{a, b} {
		for _, db := range dbs {
			if m[db.Name] == nil {
				m[db.Name] = make(map[string]struct{})
			}
			for _, frame := range db.Frames {
				m[db.Name][frame.Name] = struct{}{}
			}
		}
	}

	// Generate new schema from map.
	dbs := make([]*DBInfo, 0, len(m))
	for db, frames := range m {
		di := &DBInfo{Name: db}
		for frame := range frames {
			di.Frames = append(di.Frames, &FrameInfo{Name: frame})
		}
		sort.Sort(frameInfoSlice(di.Frames))
		dbs = append(dbs, di)
	}
	sort.Sort(dbInfoSlice(dbs))

	return dbs
}

func (db *DB) SetRemoteMaxSlice(newmax uint64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.remoteMaxSlice = newmax
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
	Frame string
	Slice uint64
}

type importData struct {
	BitmapIDs  []uint64
	ProfileIDs []uint64
}
