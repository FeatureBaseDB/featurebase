package pilosa

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/cespare/xxhash"
)

const (
	LogEntryTypeInsertColumn = 1
	LogEntryTypeInsertRow    = 2
)

const (
	defaultReplicationRetryInterval = 1 * time.Second
)

var (
	ErrTranslateStoreClosed       = errors.New("pilosa: translate store closed")
	ErrTranslateStoreReaderClosed = errors.New("pilosa: translate store reader closed")
	ErrReplicationNotSupported    = errors.New("pilosa: replication not supported")
	ErrTranslateStoreReadOnly     = errors.New("pilosa: operation not supported, translate store read only")
)

// TranslateStore is the storage for translation string-to-uint64 values.
type TranslateStore interface {
	TranslateColumnsToUint64(index string, values []string) ([]uint64, error)
	TranslateColumnToString(index string, values uint64) (string, error)

	TranslateRowsToUint64(index, frame string, values []string) ([]uint64, error)
	TranslateRowToString(index, frame string, values uint64) (string, error)

	// Returns a reader from the given offset of the raw data file.
	// The returned reader must be closed by the caller when done.
	Reader(ctx context.Context, off int64) (io.ReadCloser, error)
}

// Ensure type implements interface.
var _ TranslateStore = &TranslateFile{}

// TranslateFile is an on-disk storage engine for translating string-to-uint64 values.
type TranslateFile struct {
	mu          sync.RWMutex
	data        []byte
	file        *os.File
	w           *bufio.Writer
	n           int64
	writeNotify chan struct{}

	once    sync.Once
	wg      sync.WaitGroup
	closing chan struct{}

	cols map[string]*index
	rows map[frameKey]*index

	Path    string
	MapSize int

	// If non-nil, data is streamed from a primary and this is a read-only store.
	PrimaryTranslateStore TranslateStore

	// Delay after attempting to connect to a primary that the store will retry.
	ReplicationRetryInterval time.Duration
}

// NewTranslateFile returns a new instance of TranslateFile.
func NewTranslateFile() *TranslateFile {
	return &TranslateFile{
		writeNotify: make(chan struct{}),
		closing:     make(chan struct{}),
		cols:        make(map[string]*index),
		rows:        make(map[frameKey]*index),

		MapSize: defaultMapSize,

		ReplicationRetryInterval: defaultReplicationRetryInterval,
	}
}

func (s *TranslateFile) Open() (err error) {
	// Open writer & buffered writer.
	if err := os.MkdirAll(filepath.Dir(s.Path), 0777); err != nil {
		return err
	} else if s.file, err = os.OpenFile(s.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		return err
	}
	s.w = bufio.NewWriter(s.file)

	// Memory map data file.
	if s.data, err = syscall.Mmap(int(s.file.Fd()), 0, s.MapSize, syscall.PROT_READ, syscall.MAP_SHARED); err != nil {
		return err
	}

	// Replay the log.
	if err := s.replayEntries(); err != nil {
		return err
	}

	// Stream from primary, if available.
	if s.PrimaryTranslateStore != nil {
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.monitorReplication() }()
	}

	return nil
}

func (s *TranslateFile) Close() (err error) {
	s.once.Do(func() {
		close(s.closing)

		if s.file != nil {
			if e := s.file.Close(); e != nil && err == nil {
				err = e
			}
		}
		if s.data != nil {
			if e := syscall.Munmap(s.data); e != nil && err == nil {
				err = e
			}
		}
	})
	s.wg.Wait()
	return err
}

// Closing returns a channel that is closed when the store is closed.
func (s *TranslateFile) Closing() <-chan struct{} {
	return s.closing
}

// Size returns the number of bytes in use in the data file.
func (s *TranslateFile) Size() int64 {
	s.mu.RLock()
	n := s.n
	s.mu.RUnlock()
	return n
}

// IsReadOnly returns true if this store is being replicated from a primary store.
func (s *TranslateFile) IsReadOnly() bool {
	return s.PrimaryTranslateStore != nil
}

// WriteNotify returns a channel that is closed when a new entry is written.
func (s *TranslateFile) WriteNotify() <-chan struct{} {
	s.mu.RLock()
	ch := s.writeNotify
	s.mu.RUnlock()
	return ch
}

func (s *TranslateFile) appendEntry(entry *LogEntry) error {
	offset := s.n

	// Append entry to the end of the WAL.
	n, err := entry.WriteTo(s.w)
	if err != nil {
		return err
	} else if err := s.w.Flush(); err != nil {
		return err
	}

	// Move position forward.
	s.n += n

	// Apply the entry to the current state.
	if err := s.applyEntry(entry, offset); err != nil {
		return err
	} else if err := s.file.Sync(); err != nil {
		return err
	}

	// Notify others of write update.
	close(s.writeNotify)
	s.writeNotify = make(chan struct{})

	return nil
}

func (s *TranslateFile) applyEntry(entry *LogEntry, offset int64) error {
	// Move offset to the start of the id/key pairs.
	offset += entry.HeaderSize()

	var idx *index
	switch entry.Type {
	case LogEntryTypeInsertColumn:
		idx = s.col(string(entry.Index))

	case LogEntryTypeInsertRow:
		idx = s.row(string(entry.Index), string(entry.Frame))

	default:
		return fmt.Errorf("enterprise.TranslateFile.applyEntry(): unknown log entry type: 0x%20x", entry.Type)
	}

	// Insert id/key pairs into index.
	for i, id := range entry.IDs {
		key := entry.Keys[i]

		// Determine key offset based on ID size.
		sz := int64(uVarintSize(id))
		idx.insert(id, offset+sz)

		// Move sequence forward.
		if id > idx.seq {
			idx.seq = id
		}

		// Move offset forward.
		offset += sz + int64(uVarintSize(uint64(len(key)))) + int64(len(key))
	}

	return nil
}

func (s *TranslateFile) replayEntries() error {
	// Build a reader from the memory-map data.
	fi, err := os.Stat(s.Path)
	if err != nil {
		return err
	}
	r := bytes.NewReader(s.data[:fi.Size()])

	// Iterate over each entry and reapply.
	for {
		offset := s.n

		var entry LogEntry
		if n, err := entry.ReadFrom(r); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else {
			s.n += n
		}

		if err := s.applyEntry(&entry, offset); err != nil {
			return err
		}
	}
}

// monitorReplication is executed in a separate goroutine and continually streams
// from the primary store until this store is closed.
func (s *TranslateFile) monitorReplication() {
	// Create context that will cancel on close.
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-s.closing; cancel() }()

	// Keep attempting to replicate until the store closes.
	for {
		if err := s.replicate(ctx); err != nil {
			log.Printf("pilosa: replication error: %s", err)
		}

		select {
		case <-s.closing:
			return
		case <-time.After(s.ReplicationRetryInterval):
			log.Printf("pilosa: reconnecting to primary replica")
		}
	}
}

func (s *TranslateFile) replicate(ctx context.Context) error {
	off := s.Size()

	// Connect to remote primary.
	log.Printf("pilosa: replicating from offset %d", off)
	rc, err := s.PrimaryTranslateStore.Reader(ctx, off)
	if err != nil {
		return err
	}
	defer rc.Close()

	// Wrap in bufferred I/O so it implements io.ByteReader.
	bufr := bufio.NewReader(rc)

	// Continually read new entries from primary and append to local store.
	for {
		// Read next available entry.
		var entry LogEntry
		if _, err := entry.ReadFrom(bufr); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		s.mu.Lock()
		// Write to local store.
		if err := s.appendEntry(&entry); err != nil {
			s.mu.Unlock()
			return err
		}
		s.mu.Unlock()
	}
}

func (s *TranslateFile) col(index string) *index {
	idx := s.cols[index]
	if idx == nil {
		idx = newIndex(s.data)
		s.cols[index] = idx
	}
	return idx
}

func (s *TranslateFile) row(index, frame string) *index {
	idx := s.rows[frameKey{index, frame}]
	if idx == nil {
		idx = newIndex(s.data)
		s.rows[frameKey{index, frame}] = idx
	}
	return idx
}

// TranslateColumnsToUint64 converts values to a uint64 id.
// If value does not have an associated id then one is created.
func (s *TranslateFile) TranslateColumnsToUint64(index string, values []string) ([]uint64, error) {
	ret := make([]uint64, len(values))

	// Read value under read lock.
	s.mu.RLock()
	if idx := s.cols[index]; idx != nil {
		var writeRequired bool
		for i := range values {
			v, ok := idx.idByKey([]byte(values[i]))
			if !ok {
				writeRequired = true
			}
			ret[i] = v
		}
		if !writeRequired {
			s.mu.RUnlock()
			return ret, nil
		}
	}
	s.mu.RUnlock()

	// Return error if not all values could be translated and this store is read-only.
	if s.IsReadOnly() {
		return ret, ErrTranslateStoreReadOnly
	}

	// If any values not found then recheck and then add under a write lock.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Recheck if value was created between the read lock and write lock.
	idx := s.cols[index]
	if idx != nil {
		var writeRequired bool
		for i := range values {
			if ret[i] != 0 {
				continue
			}
			v, ok := idx.idByKey([]byte(values[i]))
			if !ok {
				writeRequired = true
				continue
			}
			ret[i] = v
		}
		if !writeRequired {
			return ret, nil
		}
	}

	// Create index map if it doesn't exists.
	if idx == nil {
		idx = newIndex(s.data)
		s.cols[index] = idx
	}

	// Append new identifiers to log.
	entry := &LogEntry{
		Type:  LogEntryTypeInsertColumn,
		Index: []byte(index),
		IDs:   make([]uint64, 0, len(values)),
		Keys:  make([][]byte, 0, len(values)),
	}

	check := make(map[string]uint64)
	for i := range values {
		if ret[i] != 0 {
			continue
		}
		v, found := check[values[i]]
		if !found {
			idx.seq++
			v = idx.seq
			check[values[i]] = v
		}

		ret[i] = v

		entry.IDs = append(entry.IDs, v)
		entry.Keys = append(entry.Keys, []byte(values[i]))
	}

	// Write entry.
	if err := s.appendEntry(entry); err != nil {
		return nil, err
	}

	return ret, nil
}

// TranslateColumnToString converts a uint64 id to its associated string value.
// If the id is not associated with a string value then a blank string is returned.
func (s *TranslateFile) TranslateColumnToString(index string, value uint64) (string, error) {
	s.mu.RLock()
	if idx := s.cols[index]; idx != nil {
		if ret, ok := idx.keyByID(value); ok {
			s.mu.RUnlock()
			return string(ret), nil
		}
	}
	s.mu.RUnlock()
	return "", nil
}

func (s *TranslateFile) TranslateRowsToUint64(index, frame string, values []string) ([]uint64, error) {
	key := frameKey{index, frame}

	ret := make([]uint64, len(values))

	// Read value under read lock.
	s.mu.RLock()
	if idx := s.rows[key]; idx != nil {
		var writeRequired bool
		for i := range values {
			v, ok := idx.idByKey([]byte(values[i]))
			if !ok {
				writeRequired = true
			}
			ret[i] = v
		}
		if !writeRequired {
			s.mu.RUnlock()
			return ret, nil
		}
	}
	s.mu.RUnlock()

	// Return error if not all values could be translated and this store is read-only.
	if s.IsReadOnly() {
		return ret, ErrTranslateStoreReadOnly
	}

	// If any values not found then recheck and then add under a write lock.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Recheck if value was created between the read lock and write lock.
	idx := s.rows[key]
	if idx != nil {
		var writeRequired bool
		for i := range values {
			if ret[i] != 0 {
				continue
			}
			v, ok := idx.idByKey([]byte(values[i]))
			if !ok {
				writeRequired = true
				continue
			}
			ret[i] = v
		}
		if !writeRequired {
			return ret, nil
		}
	}

	// Create map if it doesn't exists.
	if idx == nil {
		idx = newIndex(s.data)
		s.rows[key] = idx
	}

	// Append new identifiers to log.
	entry := &LogEntry{
		Type:  LogEntryTypeInsertRow,
		Index: []byte(index),
		Frame: []byte(frame),
		IDs:   make([]uint64, 0, len(values)),
		Keys:  make([][]byte, 0, len(values)),
	}
	check := make(map[string]uint64)
	for i := range values {
		if ret[i] != 0 {
			continue
		}

		v, found := check[values[i]]
		if !found {
			idx.seq++
			v = idx.seq
			check[values[i]] = v
		}
		ret[i] = v
		entry.IDs = append(entry.IDs, v)
		entry.Keys = append(entry.Keys, []byte(values[i]))
	}

	// Write entry.
	if err := s.appendEntry(entry); err != nil {
		return nil, err
	}

	return ret, nil
}

func (s *TranslateFile) TranslateRowToString(index, frame string, id uint64) (string, error) {
	s.mu.RLock()
	if idx := s.rows[frameKey{index, frame}]; idx != nil {
		if ret, ok := idx.keyByID(id); ok {
			s.mu.RUnlock()
			return string(ret), nil
		}
	}
	s.mu.RUnlock()
	return "", nil
}

// Reader returns a reader that streams the underlying data file.
func (s *TranslateFile) Reader(ctx context.Context, offset int64) (io.ReadCloser, error) {
	rc := NewTranslateFileReader(ctx, s, offset)
	if err := rc.Open(); err != nil {
		return nil, err
	}
	return rc, nil
}

type LogEntry struct {
	Type  uint8
	Index []byte
	Frame []byte

	IDs  []uint64
	Keys [][]byte

	// Length of the entry, in bytes.
	// This is only populated after ReadFrom() or WriteTo().
	Length uint64
}

// HeaderSize returns the number of bytes required for size, type, index, frame, & pair count.
func (e *LogEntry) HeaderSize() int64 {
	sz := uVarintSize(e.Length) + // total entry length
		1 + // type
		uVarintSize(uint64(len(e.Index))) + len(e.Index) + // Index length and data
		uVarintSize(uint64(len(e.Frame))) + len(e.Frame) + // Frame length and data
		uVarintSize(uint64(len(e.IDs))) // ID/Key pair count
	return int64(sz)
}

// ReadFrom deserializes a LogEntry from r. r must be a ByteReader.
func (e *LogEntry) ReadFrom(r io.Reader) (_ int64, err error) {
	br := r.(io.ByteReader)

	// Read the entry length.
	if e.Length, err = binary.ReadUvarint(br); err != nil {
		return int64(uVarintSize(e.Length)), err
	}

	// Slurp entire entry and replace reader.
	buf := make([]byte, e.Length)
	n, err := io.ReadFull(r, buf)
	n64 := int64(n + uVarintSize(e.Length))
	if err != nil {
		return n64, err
	}
	bufr := bytes.NewReader(buf)
	br, r = bufr, bufr

	// Read the entry type.
	if err := binary.Read(r, binary.BigEndian, &e.Type); err != nil {
		return n64, err
	}

	// Read index name.
	if sz, err := binary.ReadUvarint(br); err != nil {
		return n64, err
	} else if sz == 0 {
		e.Index = nil
	} else {
		e.Index = make([]byte, sz)
		if _, err := io.ReadFull(r, e.Index); err != nil {
			return n64, err
		}
	}

	// Read frame name.
	if sz, err := binary.ReadUvarint(br); err != nil {
		return n64, err
	} else if sz == 0 {
		e.Frame = nil
	} else {
		e.Frame = make([]byte, sz)
		if _, err := io.ReadFull(r, e.Frame); err != nil {
			return n64, err
		}
	}

	// Read key count.
	if n, err := binary.ReadUvarint(br); err != nil {
		return n64, err
	} else if n == 0 {
		e.IDs, e.Keys = nil, nil
	} else {
		e.IDs, e.Keys = make([]uint64, n), make([][]byte, n)
	}

	// Read each id/key pairs.
	for i := range e.Keys {
		// Read identifier.
		if e.IDs[i], err = binary.ReadUvarint(br); err != nil {
			return n64, err
		}

		// Read key.
		if sz, err := binary.ReadUvarint(br); err != nil {
			return n64, err
		} else if sz > 0 {
			e.Keys[i] = make([]byte, sz)
			if _, err := io.ReadFull(r, e.Keys[i]); err != nil {
				return n64, err
			}
		}
	}
	return n64, nil
}

// WriteTo serializes a LogEntry to w.
func (e *LogEntry) WriteTo(w io.Writer) (_ int64, err error) {
	var buf bytes.Buffer
	b := make([]byte, binary.MaxVarintLen64)

	// Write the entry type.
	if err := binary.Write(&buf, binary.BigEndian, e.Type); err != nil {
		return 0, err
	}

	// Write the index name.
	sz := binary.PutUvarint(b, uint64(len(e.Index)))
	if _, err := buf.Write(b[:sz]); err != nil {
		return 0, err
	} else if _, err := buf.Write(e.Index); err != nil {
		return 0, err
	}

	// Write frame name.
	sz = binary.PutUvarint(b, uint64(len(e.Frame)))
	if _, err := buf.Write(b[:sz]); err != nil {
		return 0, err
	} else if _, err := buf.Write(e.Frame); err != nil {
		return 0, err
	}

	// Write key count.
	sz = binary.PutUvarint(b, uint64(len(e.IDs)))
	if _, err := buf.Write(b[:sz]); err != nil {
		return 0, err
	}

	// Write each id/key pairs.
	for i := range e.Keys {
		// Write identifier.
		sz = binary.PutUvarint(b, e.IDs[i])
		if _, err := buf.Write(b[:sz]); err != nil {
			return 0, err
		}

		// Write key.
		sz = binary.PutUvarint(b, uint64(len(e.Keys[i])))
		if _, err := buf.Write(b[:sz]); err != nil {
			return 0, err
		} else if _, err := buf.Write(e.Keys[i]); err != nil {
			return 0, err
		}
	}

	// Write buffer size.
	e.Length = uint64(buf.Len())
	sz = binary.PutUvarint(b, e.Length)
	if n, err := w.Write(b[:sz]); err != nil {
		return int64(n), err
	}

	// Write buffer.
	n, err := buf.WriteTo(w)
	return int64(sz) + n, err
}

// validLogEntriesLen returns the maximum length of p that contains valid entries.
func validLogEntriesLen(p []byte) (n int) {
	r := bytes.NewReader(p)
	for {
		if sz, err := binary.ReadUvarint(r); err != nil {
			return n
		} else if off, err := r.Seek(int64(sz), io.SeekCurrent); err != nil {
			return n
		} else if off > int64(len(p)) {
			return n
		} else {
			n = int(off)
		}
	}
}

type frameKey struct {
	index string
	frame string
}

const defaultLoadFactor = 90

// index represents a two-way index between IDs and keys.
type index struct {
	seq  uint64 // autoincrement sequence
	data []byte // memory-mapped file containing key data

	// RHH hashmap for id-to-offset mapping.
	// This is required so we don't need to store key data on the heap.
	// https://cs.uwaterloo.ca/research/tr/1986/CS-86-14.pdf
	elems      []elem // id/offset key pairs
	n          uint64 // number of inuse elements
	mask       uint64 // mask applied for modulus
	threshold  uint64 // threshold when capacity doubles
	loadFactor int    // factor used to calculate threshold

	// Builtin hashmap for offset-to-id mapping.
	offsetsByID map[uint64]int64
}

func newIndex(data []byte) *index {
	idx := &index{
		data:        data,
		offsetsByID: make(map[uint64]int64),

		loadFactor: defaultLoadFactor,
	}
	idx.alloc(pow2(uint64(256)))
	return idx
}

// keyByID returns the key for a given ID, if it exists.
func (idx *index) keyByID(id uint64) ([]byte, bool) {
	offset, ok := idx.offsetsByID[id]
	if !ok {
		return nil, false
	}
	return idx.lookupKey(offset), true
}

// idByKey returns the ID for a given key, if it exists.
func (idx *index) idByKey(key []byte) (uint64, bool) {
	hash := hashKey(key)
	pos := hash & idx.mask

	var dist uint64
	for {
		if e := &idx.elems[pos]; e.hash == 0 {
			return 0, false
		} else if dist > idx.dist(e.hash, pos) {
			return 0, false
		} else if e.hash == hash && bytes.Equal(idx.lookupKey(e.offset), key) {
			return e.id, true
		}

		pos = (pos + 1) & idx.mask
		dist++
	}
}

// insert adds the id/offset pair to the index.
// This function will resize the map if it crosses the threshold.
func (idx *index) insert(id uint64, offset int64) {
	idx.n++

	// Add to reverse lookup.
	idx.offsetsByID[id] = offset

	// Grow the map if we've run out of slots.
	if idx.n > idx.threshold {
		elems, capacity := idx.elems, uint64(len(idx.elems))
		idx.alloc(uint64(len(idx.elems) * 2))

		for i := uint64(0); i < capacity; i++ {
			e := &elems[i]
			if e.hash == 0 {
				continue
			}
			idx.insertIDbyOffset(e.offset, e.id)
		}
	}

	// If the key was overwritten then decrement the size.
	if overwritten := idx.insertIDbyOffset(offset, id); overwritten {
		idx.n--
	}
}

// insertIDbyOffset writes to the RHH id-by-offset map.
func (idx *index) insertIDbyOffset(offset int64, id uint64) (overwritten bool) {
	key := idx.lookupKey(offset)
	hash := hashKey(key)
	pos := hash & idx.mask

	var dist uint64
	for {
		e := &idx.elems[pos]

		// Exit if a matching or empty slot exists.
		if e.hash == 0 {
			e.hash, e.offset, e.id = hash, offset, id
			return false
		} else if bytes.Equal(idx.lookupKey(e.offset), key) {
			e.hash, e.offset, e.id = hash, offset, id
			return true
		}

		// Swap if current element has a lower probe distance.
		d := idx.dist(e.hash, pos)
		if d < dist {
			hash, e.hash = e.hash, hash
			offset, e.offset = e.offset, offset
			id, e.id = e.id, id
			dist = d
		}

		// Move position forward.
		pos = (pos + 1) & idx.mask
		dist++
	}
}

// lookupKey returns the key at the given offset in the memory-mapped file.
func (idx *index) lookupKey(offset int64) []byte {
	data := idx.data[offset:]
	n, sz := binary.Uvarint(data)
	if sz == 0 {
		return nil
	}
	return data[sz : sz+int(n)]
}

func (idx *index) alloc(capacity uint64) {
	idx.elems = make([]elem, capacity)
	idx.threshold = (capacity * uint64(idx.loadFactor)) / 100
	idx.mask = uint64(capacity - 1)
}

func (idx *index) dist(hash, i uint64) uint64 {
	return (i + uint64(len(idx.elems)) - (hash & idx.mask)) & idx.mask
}

type elem struct {
	offset int64
	id     uint64
	hash   uint64
}

func (e *elem) reset() {
	e.offset = 0
	e.id = 0
	e.hash = 0
}

func hashKey(key []byte) uint64 {
	h := xxhash.Sum64(key)
	if h == 0 {
		h = 1
	}
	return h
}

func pow2(v uint64) uint64 {
	for i := uint64(2); i < 1<<62; i *= 2 {
		if i >= v {
			return i
		}
	}
	panic("unreachable")
}

// TranslateFileReader implements a reader that continuously streams data from a store.
type TranslateFileReader struct {
	ctx    context.Context
	store  *TranslateFile
	file   *os.File
	offset int64
	notify <-chan struct{}

	once    sync.Once
	closing chan struct{}
}

// NewTranslateFileReader returns a new instance of TranslateFileReader.
func NewTranslateFileReader(ctx context.Context, store *TranslateFile, offset int64) *TranslateFileReader {
	return &TranslateFileReader{
		ctx:     ctx,
		store:   store,
		offset:  offset,
		notify:  store.WriteNotify(),
		closing: make(chan struct{}),
	}
}

// Open initializes the reader.
func (r *TranslateFileReader) Open() (err error) {
	if r.file, err = os.Open(r.store.Path); err != nil {
		return err
	}
	return nil
}

// Close closes the underlying file reader.
func (r *TranslateFileReader) Close() error {
	r.once.Do(func() { close(r.closing) })

	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// Read reads the next section of the available data to p. This should always
// read from the start of an entry and read n bytes to the end of another entry.
func (r *TranslateFileReader) Read(p []byte) (n int, err error) {
	for {
		// Obtain notification channel before we check for new data.
		notify := r.store.WriteNotify()

		// Exit if we can read one or more valid entries or we receive an error.
		if n, err = r.read(p); n > 0 || err != nil {
			return n, err
		}

		// Wait for new data or close.
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case <-r.closing:
			return 0, ErrTranslateStoreReaderClosed
		case <-r.store.Closing():
			return 0, ErrTranslateStoreClosed
		case <-notify:
			continue
		}
	}
}

// read writes the bytes for zero or more valid entries to p.
func (r *TranslateFileReader) read(p []byte) (n int, err error) {
	sz := r.store.Size()

	// Exit if there is no new data.
	if sz < r.offset {
		return 0, fmt.Errorf("pilosa: translate store reader past file size: sz=%d off=%d", sz, r.offset)
	} else if sz == r.offset {
		return 0, nil
	}

	// Shorten buffer to maximum read size.
	if max := sz - r.offset; int64(len(p)) > max {
		p = p[:max]
	}

	// Read data from file at offset.
	// Limit the number of bytes read to only whole entries.
	n, err = r.file.ReadAt(p, r.offset)
	n = validLogEntriesLen(p[:n])
	r.offset += int64(n)
	return n, err
}

// Copied & modified from encoding/binary.
func uVarintSize(x uint64) (i int) {
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return i + 1
}
