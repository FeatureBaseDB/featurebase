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

// Default database settings.
const (
	DefaultColumnLabel = "profileID"
)

// DB represents a container for frames.
type DB struct {
	mu   sync.Mutex
	path string
	name string

	// Default time quantum for all frames in database.
	// This can be overridden by individual frames.
	timeQuantum TimeQuantum

	// Label used for referring to columns in database.
	columnLabel string

	// Frames by name.
	frames map[string]*Frame

	// Max Slice on any node in the cluster, according to this node
	remoteMaxSlice        uint64
	remoteMaxInverseSlice uint64

	// Profile attribute storage and cache
	profileAttrStore *AttrStore

	stats StatsClient

	LogOutput io.Writer
}

// NewDB returns a new instance of DB.
func NewDB(path, name string) (*DB, error) {
	err := ValidateName(name)
	if err != nil {
		return nil, err
	}

	return &DB{
		path:   path,
		name:   name,
		frames: make(map[string]*Frame),

		remoteMaxSlice:        0,
		remoteMaxInverseSlice: 0,

		profileAttrStore: NewAttrStore(filepath.Join(path, ".data")),

		columnLabel: DefaultColumnLabel,

		stats:     NopStatsClient,
		LogOutput: ioutil.Discard,
	}, nil
}

// Name returns name of the database.
func (db *DB) Name() string { return db.name }

// Path returns the path the database was initialized with.
func (db *DB) Path() string { return db.path }

// ProfileAttrStore returns the storage for profile attributes.
func (db *DB) ProfileAttrStore() *AttrStore { return db.profileAttrStore }

// SetColumnLabel sets the column label. Persists to meta file on update.
func (db *DB) SetColumnLabel(v string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Ignore if no change occurred.
	if v == "" || db.columnLabel == v {
		return nil
	}

	// Persist meta data to disk on change.
	db.columnLabel = v
	if err := db.saveMeta(); err != nil {
		return err
	}

	return nil
}

// ColumnLabel returns the column label.
func (db *DB) ColumnLabel() string {
	db.mu.Lock()
	v := db.columnLabel
	db.mu.Unlock()
	return v
}

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

		fr, err := db.newFrame(db.FramePath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
		if err != nil {
			return ErrName
		}
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
	buf, err := ioutil.ReadFile(filepath.Join(db.path, ".meta"))
	if os.IsNotExist(err) {
		db.timeQuantum = ""
		db.columnLabel = DefaultColumnLabel
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
	db.columnLabel = pb.ColumnLabel

	return nil
}

// saveMeta writes meta data for the database.
func (db *DB) saveMeta() error {
	// Marshal metadata.
	buf, err := proto.Marshal(&internal.DB{
		TimeQuantum: string(db.timeQuantum),
		ColumnLabel: db.columnLabel,
	})
	if err != nil {
		return err
	}

	// Write to meta file.
	if err := ioutil.WriteFile(filepath.Join(db.path, ".meta"), buf, 0666); err != nil {
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

func (db *DB) SetRemoteMaxSlice(v uint64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.remoteMaxSlice = v
}

// MaxInverseSlice returns the max inverse slice in the database according to this node.
func (db *DB) MaxInverseSlice() uint64 {
	if db == nil {
		return 0
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	max := db.remoteMaxInverseSlice
	for _, f := range db.frames {
		if slice := f.MaxInverseSlice(); slice > max {
			max = slice
		}
	}
	return max
}

func (db *DB) SetRemoteMaxInverseSlice(v uint64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.remoteMaxInverseSlice = v
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

// CreateFrame creates a frame.
func (db *DB) CreateFrame(name string, opt FrameOptions) (*Frame, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Ensure frame doesn't already exist.
	if db.frames[name] != nil {
		return nil, ErrFrameExists
	}
	return db.createFrame(name, opt)
}

// CreateFrameIfNotExists creates a frame with the given options if it doesn't exist.
func (db *DB) CreateFrameIfNotExists(name string, opt FrameOptions) (*Frame, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Find frame in cache first.
	if f := db.frames[name]; f != nil {
		return f, nil
	}

	return db.createFrame(name, opt)
}

func (db *DB) createFrame(name string, opt FrameOptions) (*Frame, error) {

	if name == "" {
		return nil, errors.New("frame name required")
	}

	// Initialize frame.
	f, err := db.newFrame(db.FramePath(name), name)
	if err != nil {
		return nil, err
	}

	// Open frame.
	if err := f.Open(); err != nil {
		return nil, err
	}

	// Default the time quantum to what is set on the DB.
	if err := f.SetTimeQuantum(db.timeQuantum); err != nil {
		f.Close()
		return nil, err
	}

	// Set options.
	if opt.RowLabel != "" {
		f.rowLabel = opt.RowLabel
	}
	f.inverseEnabled = opt.InverseEnabled
	if err := f.saveMeta(); err != nil {
		f.Close()
		return nil, err
	}

	// Add to database's frame lookup.
	db.frames[name] = f

	db.stats.Count("frameN", 1)

	return f, nil
}

func (db *DB) newFrame(path, name string) (*Frame, error) {
	f, err := NewFrame(path, db.name, name)
	if err != nil {
		return nil, err
	}
	f.LogOutput = db.LogOutput
	f.stats = db.stats.WithTags(fmt.Sprintf("frame:%s", name))
	return f, nil
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
	m := make(map[string]map[string]map[string]struct{})
	for _, dbs := range [][]*DBInfo{a, b} {
		for _, db := range dbs {
			if m[db.Name] == nil {
				m[db.Name] = make(map[string]map[string]struct{})
			}
			for _, frame := range db.Frames {
				if m[db.Name][frame.Name] == nil {
					m[db.Name][frame.Name] = make(map[string]struct{})
				}
				for _, view := range frame.Views {
					m[db.Name][frame.Name][view.Name] = struct{}{}
				}
			}
		}
	}

	// Generate new schema from map.
	dbs := make([]*DBInfo, 0, len(m))
	for db, frames := range m {
		di := &DBInfo{Name: db}
		for frame, views := range frames {
			fi := &FrameInfo{Name: frame}
			for view := range views {
				fi.Views = append(fi.Views, &ViewInfo{Name: view})
			}
			sort.Sort(viewInfoSlice(fi.Views))
			di.Frames = append(di.Frames, fi)
		}
		sort.Sort(frameInfoSlice(di.Frames))
		dbs = append(dbs, di)
	}
	sort.Sort(dbInfoSlice(dbs))

	return dbs
}

// DBOptions represents options to set when initializing a db.
type DBOptions struct {
	ColumnLabel string `json:"columnLabel,omitempty"`
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
	BitmapIDs  []uint64
	ProfileIDs []uint64
}
