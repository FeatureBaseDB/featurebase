package pilosa

import (
	"context"
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
	mu sync.Mutex

	// Databases by name.
	dbs map[string]*DB

	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	// Stats
	Stats StatsClient

	// Data directory path.
	Path string

	// The interval at which the cached bitmap ids are persisted to disk.
	CacheFlushInterval time.Duration

	LogOutput io.Writer
}

// NewIndex returns a new instance of Index.
func NewIndex() *Index {
	return &Index{
		dbs:     make(map[string]*DB),
		closing: make(chan struct{}, 0),

		Stats: NopStatsClient,

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

		db, err := i.newDB(i.DBPath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
		if err != nil {
			return ErrName
		}
		if err := db.Open(); err != nil {
			return fmt.Errorf("open db: name=%s, err=%s", db.Name(), err)
		}
		i.dbs[db.Name()] = db

		i.Stats.Count("dbN", 1)
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

// MaxSlices returns MaxSlice map for all databases.
func (i *Index) MaxSlices() map[string]uint64 {
	a := make(map[string]uint64)
	for _, db := range i.DBs() {
		a[db.Name()] = db.MaxSlice()
	}
	return a
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

// CreateDB creates a database.
func (i *Index) CreateDB(name string, opt DBOptions) (*DB, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Ensure db doesn't already exist.
	if i.dbs[name] != nil {
		return nil, ErrDatabaseExists
	}
	return i.createDB(name, opt)
}

// CreateDBIfNotExists returns a database by name.
// The database is created if it does not already exist.
func (i *Index) CreateDBIfNotExists(name string, opt DBOptions) (*DB, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Find frame in cache first.
	if db := i.dbs[name]; db != nil {
		return db, nil
	}

	return i.createDB(name, opt)
}

func (i *Index) createDB(name string, opt DBOptions) (*DB, error) {
	if name == "" {
		return nil, errors.New("database name required")
	}

	// Return database if it exists.
	if db := i.db(name); db != nil {
		return db, nil
	}

	// Otherwise create a new database.
	db, err := i.newDB(i.DBPath(name), name)
	if err != nil {
		return nil, err
	}

	if err := db.Open(); err != nil {
		return nil, err
	}

	// Update options.
	db.SetColumnLabel(opt.ColumnLabel)

	i.dbs[db.Name()] = db

	i.Stats.Count("dbN", 1)

	return db, nil
}

func (i *Index) newDB(path, name string) (*DB, error) {
	db, err := NewDB(path, name)
	if err != nil {
		return nil, err
	}
	db.LogOutput = i.LogOutput
	db.stats = i.Stats.WithTags(fmt.Sprintf("db:%s", db.Name()))
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

	i.Stats.Count("dbN", -1)

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

// Fragment returns the fragment for a database, frame & slice.
func (i *Index) Fragment(db, frame string, slice uint64) *Fragment {
	f := i.Frame(db, frame)
	if f == nil {
		return nil
	}
	return f.Fragment(slice)
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

			for slice := uint64(0); slice <= s.Index.DB(di.Name).MaxSlice(); slice++ {
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
		m, err := client.ProfileAttrDiff(context.Background(), db, blks)
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
		m, err := client.BitmapAttrDiff(context.Background(), db, name, blks)
		if err == ErrFrameNotFound {
			continue // frame not created remotely yet, skip
		} else if err != nil {
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
	// Retrieve local frame.
	f := s.Index.Frame(db, frame)
	if f == nil {
		return ErrFrameNotFound
	}

	// Ensure fragment exists locally.
	frag, err := f.CreateFragmentIfNotExists(slice)
	if err != nil {
		return err
	}

	// Sync fragments together.
	fs := FragmentSyncer{
		Fragment: frag,
		Host:     s.Host,
		Cluster:  s.Cluster,
		Closing:  s.Closing,
	}
	if err := fs.SyncFragment(); err != nil {
		return err
	}

	return nil
}
