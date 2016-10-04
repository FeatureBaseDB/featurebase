package pilosa

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// DefaultCacheFlushInterval is the default value for Fragment.CacheFlushInterval.
const DefaultCacheFlushInterval = 1 * time.Minute

// Index represents a container for fragments.
type Index struct {
	mu        sync.Mutex
	remoteMax uint64

	// Databases by name.
	dbs map[string]*DB

	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	// Data directory path.
	Path string

	// The interval at which the cached bitmap ids are persisted to disk.
	CacheFlushInterval time.Duration

	LogOutput io.Writer
}

// NewIndex returns a new instance of Index.
func NewIndex() *Index {
	return &Index{
		dbs:       make(map[string]*DB),
		remoteMax: 0,
		closing:   make(chan struct{}, 0),

		CacheFlushInterval: DefaultCacheFlushInterval,

		LogOutput: os.Stderr,
	}
}

// Open initializes the root data directory for the index.
func (i *Index) Open() error {
	if err := os.MkdirAll(i.Path, 0777); err != nil {
		return err
	}

	// Open path to read all database directories.
	f, err := os.Open(i.Path)
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

		i.logger().Printf("opening database: %s", filepath.Base(fi.Name()))

		db := NewDB(i.DBPath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
		if err := db.Open(); err != nil {
			return fmt.Errorf("open db: name=%s, err=%s", db.Name(), err)
		}
		i.dbs[db.Name()] = db
	}

	// Periodically flush cache.
	i.wg.Add(1)
	go func() { defer i.wg.Done(); i.monitorCacheFlush() }()

	return nil
}

// Close closes all open fragments.
func (i *Index) Close() error {
	// Notify goroutines of closing and wait for completion.
	close(i.closing)
	i.wg.Wait()

	for _, db := range i.dbs {
		db.Close()
	}
	return nil
}

// SliceN returns the highest slice across all frames.
func (i *Index) SliceN() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()

	sliceN := i.remoteMax
	for _, db := range i.dbs {
		if n := db.SliceN(); n > sliceN {
			sliceN = n
		}
	}
	return sliceN
}

// Schema returns schema data for all databases and frames.
func (i *Index) Schema() []*DBInfo {
	var a []*DBInfo
	for _, db := range i.DBs() {
		di := &DBInfo{Name: db.Name()}
		for _, frame := range db.Frames() {
			di.Frames = append(di.Frames, &FrameInfo{
				Name: frame.Name(),
			})
		}
		a = append(a, di)
	}
	return a
}

// DBPath returns the path where a given database is stored.
func (i *Index) DBPath(name string) string { return filepath.Join(i.Path, name) }

// DB returns the database by name.
func (i *Index) DB(name string) *DB {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.db(name)
}

func (i *Index) db(name string) *DB { return i.dbs[name] }

// DBs returns a list of all databases in the index.
func (i *Index) DBs() []*DB {
	i.mu.Lock()
	defer i.mu.Unlock()

	a := make([]*DB, 0, len(i.dbs))
	for _, db := range i.dbs {
		a = append(a, db)
	}
	sort.Sort(dbSlice(a))

	return a
}

// CreateDBIfNotExists returns a database by name.
// The database is created if it does not already exist.
func (i *Index) CreateDBIfNotExists(name string) (*DB, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.createDBIfNotExists(name)
}

func (i *Index) createDBIfNotExists(name string) (*DB, error) {
	if name == "" {
		return nil, errors.New("database name required")
	}

	// Return database if it exists.
	if db := i.db(name); db != nil {
		return db, nil
	}

	// Otherwise create a new database.
	db := NewDB(i.DBPath(name), name)
	if err := db.Open(); err != nil {
		return nil, err
	}
	i.dbs[db.Name()] = db

	return db, nil
}

// DeleteDB removes a database from the index.
func (i *Index) DeleteDB(name string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Ignore if database doesn't exist.
	db := i.db(name)
	if db == nil {
		return nil
	}

	// Close database.
	if err := db.Close(); err != nil {
		return err
	}

	// Delete database directory.
	if err := os.RemoveAll(i.DBPath(name)); err != nil {
		return err
	}

	// Remove reference.
	delete(i.dbs, name)

	return nil
}

// Frame returns the frame for a database and name.
func (i *Index) Frame(db, name string) *Frame {
	d := i.DB(db)
	if d == nil {
		return nil
	}
	return d.Frame(name)
}

// CreateFrameIfNotExists returns the frame for a database & name.
// The frame is created if it doesn't already exist.
func (i *Index) CreateFrameIfNotExists(db, name string) (*Frame, error) {
	d, err := i.CreateDBIfNotExists(db)
	if err != nil {
		return nil, err
	}
	return d.CreateFrameIfNotExists(name)
}

// Fragment returns the fragment for a database, frame & slice.
func (i *Index) Fragment(db, frame string, slice uint64) *Fragment {
	f := i.Frame(db, frame)
	if f == nil {
		return nil
	}
	return f.fragment(slice)
}

// CreateFragmentIfNotExists returns the fragment for a database, frame & slice.
// The fragment is created if it doesn't already exist.
func (i *Index) CreateFragmentIfNotExists(db, frame string, slice uint64) (*Fragment, error) {
	f, err := i.CreateFrameIfNotExists(db, frame)
	if err != nil {
		return nil, err
	}
	return f.CreateFragmentIfNotExists(slice)
}

func (i *Index) SetMax(newmax uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.remoteMax = newmax
}

// monitorCacheFlush periodically flushes all fragment caches sequentially.
// This is run in a goroutine.
func (i *Index) monitorCacheFlush() {
	ticker := time.NewTicker(i.CacheFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-i.closing:
			return
		case <-ticker.C:
			i.flushCaches()
		}
	}
}

func (i *Index) flushCaches() {
	for _, db := range i.DBs() {
		for _, frame := range db.Frames() {
			for _, fragment := range frame.Fragments() {
				select {
				case <-i.closing:
					return
				default:
				}

				if err := fragment.FlushCache(); err != nil {
					i.logger().Printf("error flushing cache: err=%s, path=%s", err, fragment.CachePath())
				}
			}
		}
	}
}

func (i *Index) logger() *log.Logger { return log.New(i.LogOutput, "", log.LstdFlags) }

// IndexSyncer is an active anti-entropy tool that compares the local index
// with a remote index based on block checksums and resolves differences.
type IndexSyncer struct {
	Index *Index

	Host    string
	Cluster *Cluster

	// Signals that the sync should stop.
	Closing <-chan struct{}
}

// Returns true if the syncer has been marked to close.
func (s *IndexSyncer) IsClosing() bool {
	select {
	case <-s.Closing:
		return true
	default:
		return false
	}
}

// SyncIndex compares the index on host with the local index and resolves differences.
func (s *IndexSyncer) SyncIndex() error {
	sliceN := s.Index.SliceN()

	// Iterate over schema in sorted order.
	for _, di := range s.Index.Schema() {
		// Verify syncer has not closed.
		if s.IsClosing() {
			return nil
		}

		// Sync database profile attributes.
		if err := s.syncDatabase(di.Name); err != nil {
			return fmt.Errorf("db sync error: db=%s, err=%s", di.Name, err)
		}

		for _, fi := range di.Frames {
			// Verify syncer has not closed.
			if s.IsClosing() {
				return nil
			}

			// Sync frame bitmap attributes.
			if err := s.syncFrame(di.Name, fi.Name); err != nil {
				return fmt.Errorf("frame sync error: db=%s, frame=%s, err=%s", di.Name, fi.Name, err)
			}

			for slice := uint64(0); slice <= sliceN; slice++ {
				// Ignore slices that this host doesn't own.
				if !s.Cluster.OwnsFragment(s.Host, di.Name, slice) {
					continue
				}

				// Verify syncer has not closed.
				if s.IsClosing() {
					return nil
				}

				// Sync fragment if own it.
				if err := s.syncFragment(di.Name, fi.Name, slice); err != nil {
					return fmt.Errorf("fragment sync error: db=%s, frame=%s, slice=%d, err=%s", di.Name, fi.Name, slice, err)
				}
			}
		}
	}

	return nil
}

// syncDatabase synchronizes database attributes with the rest of the cluster.
func (s *IndexSyncer) syncDatabase(db string) error {
	// Retrieve database reference.
	d := s.Index.DB(db)
	if d == nil {
		return nil
	}

	// Read block checksums.
	blks, err := d.ProfileAttrStore().Blocks()
	if err != nil {
		return err
	}

	// Sync with every other host.
	for _, node := range Nodes(s.Cluster.Nodes).FilterHost(s.Host) {
		client, err := NewClient(node.Host)
		if err != nil {
			return err
		}

		// Retrieve attributes from differing blocks.
		// Skip update and recomputation if no attributes have changed.
		m, err := client.ProfileAttrDiff(db, blks)
		if err != nil {
			return err
		} else if len(m) == 0 {
			continue
		}

		// Update local copy.
		if err := d.ProfileAttrStore().SetBulkAttrs(m); err != nil {
			return err
		}

		// Recompute blocks.
		blks, err = d.ProfileAttrStore().Blocks()
		if err != nil {
			return err
		}
	}

	return nil
}

// syncFrame synchronizes frame attributes with the rest of the cluster.
func (s *IndexSyncer) syncFrame(db, name string) error {
	// Retrieve database reference.
	f := s.Index.Frame(db, name)
	if f == nil {
		return nil
	}

	// Read block checksums.
	blks, err := f.BitmapAttrStore().Blocks()
	if err != nil {
		return err
	}

	// Sync with every other host.
	for _, node := range Nodes(s.Cluster.Nodes).FilterHost(s.Host) {
		client, err := NewClient(node.Host)
		if err != nil {
			return err
		}

		// Retrieve attributes from differing blocks.
		// Skip update and recomputation if no attributes have changed.
		m, err := client.BitmapAttrDiff(db, name, blks)
		if err != nil {
			return err
		} else if len(m) == 0 {
			continue
		}

		// Update local copy.
		if err := f.BitmapAttrStore().SetBulkAttrs(m); err != nil {
			return err
		}

		// Recompute blocks.
		blks, err = f.BitmapAttrStore().Blocks()
		if err != nil {
			return err
		}
	}

	return nil
}

// syncFragment synchronizes a fragment with the rest of the cluster.
func (s *IndexSyncer) syncFragment(db, frame string, slice uint64) error {
	// Ensure fragment exists locally.
	f, err := s.Index.CreateFragmentIfNotExists(db, frame, slice)
	if err != nil {
		return err
	}

	// Sync fragments together.
	fs := FragmentSyncer{
		Fragment: f,
		Host:     s.Host,
		Cluster:  s.Cluster,
		Closing:  s.Closing,
	}
	if err := fs.SyncFragment(); err != nil {
		return err
	}

	return nil
}
