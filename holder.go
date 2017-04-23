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

// Holder represents a container for indexes.
type Holder struct {
	mu sync.Mutex

	// Databases by name.
	dbs map[string]*DB

	Broadcaster Broadcaster
	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	// Stats
	Stats StatsClient

	// Data directory path.
	Path string

	// The interval at which the cached row ids are persisted to disk.
	CacheFlushInterval time.Duration

	LogOutput io.Writer
}

// NewHolder returns a new instance of Holder.
func NewHolder() *Holder {
	return &Holder{
		dbs:     make(map[string]*DB),
		closing: make(chan struct{}, 0),

		Stats: NopStatsClient,

		CacheFlushInterval: DefaultCacheFlushInterval,

		LogOutput: os.Stderr,
	}
}

// Open initializes the root data directory for the holder.
func (h *Holder) Open() error {
	if err := os.MkdirAll(h.Path, 0777); err != nil {
		return err
	}

	// Open path to read all database directories.
	f, err := os.Open(h.Path)
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

		h.logger().Printf("opening database: %s", filepath.Base(fi.Name()))

		db, err := h.newDB(h.DBPath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
		if err == ErrName {
			h.logger().Printf("ERROR opening database: %s, err=%s", fi.Name(), err)
			continue
		} else if err != nil {
			return err
		}
		if err := db.Open(); err != nil {
			if err == ErrName {
				h.logger().Printf("ERROR opening database: %s, err=%s", db.Name(), err)
				continue
			}
			return fmt.Errorf("open db: name=%s, err=%s", db.Name(), err)
		}
		h.dbs[db.Name()] = db

		h.Stats.Count("dbN", 1)
	}

	// Periodically flush cache.
	h.wg.Add(1)
	go func() { defer h.wg.Done(); h.monitorCacheFlush() }()

	return nil
}

// Close closes all open fragments.
func (h *Holder) Close() error {
	// Notify goroutines of closing and wait for completion.
	close(h.closing)
	h.wg.Wait()

	for _, db := range h.dbs {
		db.Close()
	}
	return nil
}

// MaxSlices returns MaxSlice map for all databases.
func (h *Holder) MaxSlices() map[string]uint64 {
	a := make(map[string]uint64)
	for _, db := range h.DBs() {
		a[db.Name()] = db.MaxSlice()
	}
	return a
}

// MaxInverseSlices returns MaxInverseSlice map for all databases.
func (h *Holder) MaxInverseSlices() map[string]uint64 {
	a := make(map[string]uint64)
	for _, db := range h.DBs() {
		a[db.Name()] = db.MaxInverseSlice()
	}
	return a
}

// Schema returns schema data for all databases and frames.
func (h *Holder) Schema() []*DBInfo {
	var a []*DBInfo
	for _, db := range h.DBs() {
		di := &DBInfo{Name: db.Name()}
		for _, frame := range db.Frames() {
			fi := &FrameInfo{Name: frame.Name()}
			for _, view := range frame.Views() {
				fi.Views = append(fi.Views, &ViewInfo{Name: view.Name()})
			}
			sort.Sort(viewInfoSlice(fi.Views))
			di.Frames = append(di.Frames, fi)
		}
		sort.Sort(frameInfoSlice(di.Frames))
		a = append(a, di)
	}
	sort.Sort(dbInfoSlice(a))
	return a
}

// DBPath returns the path where a given database is stored.
func (h *Holder) DBPath(name string) string { return filepath.Join(h.Path, name) }

// DB returns the database by name.
func (h *Holder) DB(name string) *DB {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.db(name)
}

func (h *Holder) db(name string) *DB { return h.dbs[name] }

// DBs returns a list of all databases in the holder.
func (h *Holder) DBs() []*DB {
	h.mu.Lock()
	defer h.mu.Unlock()

	a := make([]*DB, 0, len(h.dbs))
	for _, db := range h.dbs {
		a = append(a, db)
	}
	sort.Sort(dbSlice(a))

	return a
}

// CreateDB creates a database.
// An error is returned if the database already exists.
func (h *Holder) CreateDB(name string, opt DBOptions) (*DB, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Ensure db doesn't already exist.
	if h.dbs[name] != nil {
		return nil, ErrDatabaseExists
	}
	return h.createDB(name, opt)
}

// CreateDBIfNotExists returns a database by name.
// The database is created if it does not already exist.
func (h *Holder) CreateDBIfNotExists(name string, opt DBOptions) (*DB, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Find database in cache first.
	if db := h.dbs[name]; db != nil {
		return db, nil
	}

	return h.createDB(name, opt)
}

func (h *Holder) createDB(name string, opt DBOptions) (*DB, error) {
	if name == "" {
		return nil, errors.New("database name required")
	}

	// Return database if it exists.
	if db := h.db(name); db != nil {
		return db, nil
	}

	// Otherwise create a new database.
	db, err := h.newDB(h.DBPath(name), name)
	if err != nil {
		return nil, err
	}

	if err := db.Open(); err != nil {
		return nil, err
	}

	// Update options.
	db.SetColumnLabel(opt.ColumnLabel)
	db.SetTimeQuantum(opt.TimeQuantum)

	h.dbs[db.Name()] = db

	h.Stats.Count("dbN", 1)

	return db, nil
}

func (h *Holder) newDB(path, name string) (*DB, error) {
	db, err := NewDB(path, name)
	if err != nil {
		return nil, err
	}
	db.LogOutput = h.LogOutput
	db.stats = h.Stats.WithTags(fmt.Sprintf("db:%s", db.Name()))
	db.broadcaster = h.Broadcaster
	return db, nil
}

// DeleteDB removes a database from the holder.
func (h *Holder) DeleteDB(name string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Ignore if database doesn't exist.
	db := h.db(name)
	if db == nil {
		return nil
	}

	// Close database.
	if err := db.Close(); err != nil {
		return err
	}

	// Delete database directory.
	if err := os.RemoveAll(h.DBPath(name)); err != nil {
		return err
	}

	// Remove reference.
	delete(h.dbs, name)

	h.Stats.Count("dbN", -1)

	return nil
}

// Frame returns the frame for a database and name.
func (h *Holder) Frame(db, name string) *Frame {
	d := h.DB(db)
	if d == nil {
		return nil
	}
	return d.Frame(name)
}

// View returns the view for a database, frame, and name.
func (h *Holder) View(db, frame, name string) *View {
	f := h.Frame(db, frame)
	if f == nil {
		return nil
	}
	return f.View(name)
}

// Fragment returns the fragment for a database, frame & slice.
func (h *Holder) Fragment(db, frame, view string, slice uint64) *Fragment {
	v := h.View(db, frame, view)
	if v == nil {
		return nil
	}
	return v.Fragment(slice)
}

// monitorCacheFlush periodically flushes all fragment caches sequentially.
// This is run in a goroutine.
func (h *Holder) monitorCacheFlush() {
	ticker := time.NewTicker(h.CacheFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.closing:
			return
		case <-ticker.C:
			h.flushCaches()
		}
	}
}

func (h *Holder) flushCaches() {
	for _, db := range h.DBs() {
		for _, frame := range db.Frames() {
			for _, view := range frame.Views() {
				for _, fragment := range view.Fragments() {
					select {
					case <-h.closing:
						return
					default:
					}

					if err := fragment.FlushCache(); err != nil {
						h.logger().Printf("error flushing cache: err=%s, path=%s", err, fragment.CachePath())
					}
				}
			}
		}
	}
}

func (h *Holder) logger() *log.Logger { return log.New(h.LogOutput, "", log.LstdFlags) }

// HolderSyncer is an active anti-entropy tool that compares the local holder
// with a remote holder based on block checksums and resolves differences.
type HolderSyncer struct {
	Holder *Holder

	Host    string
	Cluster *Cluster

	// Signals that the sync should stop.
	Closing <-chan struct{}
}

// Returns true if the syncer has been marked to close.
func (s *HolderSyncer) IsClosing() bool {
	select {
	case <-s.Closing:
		return true
	default:
		return false
	}
}

// SyncHolder compares the holder on host with the local holder and resolves differences.
func (s *HolderSyncer) SyncHolder() error {
	// Iterate over schema in sorted order.
	for _, di := range s.Holder.Schema() {
		// Verify syncer has not closed.
		if s.IsClosing() {
			return nil
		}

		// Sync database column attributes.
		if err := s.syncDatabase(di.Name); err != nil {
			return fmt.Errorf("db sync error: db=%s, err=%s", di.Name, err)
		}

		for _, fi := range di.Frames {
			// Verify syncer has not closed.
			if s.IsClosing() {
				return nil
			}

			// Sync frame row attributes.
			if err := s.syncFrame(di.Name, fi.Name); err != nil {
				return fmt.Errorf("frame sync error: db=%s, frame=%s, err=%s", di.Name, fi.Name, err)
			}

			for _, vi := range fi.Views {
				// Verify syncer has not closed.
				if s.IsClosing() {
					return nil
				}

				for slice := uint64(0); slice <= s.Holder.DB(di.Name).MaxSlice(); slice++ {
					// Ignore slices that this host doesn't own.
					if !s.Cluster.OwnsFragment(s.Host, di.Name, slice) {
						continue
					}

					// Verify syncer has not closed.
					if s.IsClosing() {
						return nil
					}

					// Sync fragment if own it.
					if err := s.syncFragment(di.Name, fi.Name, vi.Name, slice); err != nil {
						return fmt.Errorf("fragment sync error: db=%s, frame=%s, slice=%d, err=%s", di.Name, fi.Name, slice, err)
					}
				}
			}
		}
	}

	return nil
}

// syncDatabase synchronizes database attributes with the rest of the cluster.
func (s *HolderSyncer) syncDatabase(db string) error {
	// Retrieve database reference.
	d := s.Holder.DB(db)
	if d == nil {
		return nil
	}

	// Read block checksums.
	blks, err := d.ColumnAttrStore().Blocks()
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
		m, err := client.ColumnAttrDiff(context.Background(), db, blks)
		if err != nil {
			return err
		} else if len(m) == 0 {
			continue
		}

		// Update local copy.
		if err := d.ColumnAttrStore().SetBulkAttrs(m); err != nil {
			return err
		}

		// Recompute blocks.
		blks, err = d.ColumnAttrStore().Blocks()
		if err != nil {
			return err
		}
	}

	return nil
}

// syncFrame synchronizes frame attributes with the rest of the cluster.
func (s *HolderSyncer) syncFrame(db, name string) error {
	// Retrieve database reference.
	f := s.Holder.Frame(db, name)
	if f == nil {
		return nil
	}

	// Read block checksums.
	blks, err := f.RowAttrStore().Blocks()
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
		m, err := client.RowAttrDiff(context.Background(), db, name, blks)
		if err == ErrFrameNotFound {
			continue // frame not created remotely yet, skip
		} else if err != nil {
			return err
		} else if len(m) == 0 {
			continue
		}

		// Update local copy.
		if err := f.RowAttrStore().SetBulkAttrs(m); err != nil {
			return err
		}

		// Recompute blocks.
		blks, err = f.RowAttrStore().Blocks()
		if err != nil {
			return err
		}
	}

	return nil
}

// syncFragment synchronizes a fragment with the rest of the cluster.
func (s *HolderSyncer) syncFragment(db, frame, view string, slice uint64) error {
	// Retrieve local frame.
	f := s.Holder.Frame(db, frame)
	if f == nil {
		return ErrFrameNotFound
	}

	// Ensure view exists locally.
	v, err := f.CreateViewIfNotExists(view)
	if err != nil {
		return err
	}

	// Ensure fragment exists locally.
	frag, err := v.CreateFragmentIfNotExists(slice)
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
