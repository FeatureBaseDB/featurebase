package csv

import (
	"context"
	"encoding/csv"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/pkg/errors"
)

type Source struct {
	Files        chan string
	Header       []string
	IgnoreHeader bool
	JustDoIt     bool
	Log          logger.Logger

	schemaLock sync.Mutex
	schema     []idk.Field

	records      chan Record
	once         *sync.Once
	expectHeader bool
}

func (s *Source) Record() (idk.Record, error) {
	s.once.Do(func() { go s.run() })

	rec, ok := <-s.records
	if !ok {
		return nil, io.EOF
	}
	return rec, rec.err
}

type Record struct {
	data []interface{}
	err  error
}

func (r Record) Data() []interface{} {
	return r.data
}
func (r Record) Commit(ctx context.Context) error { return nil } // TODO do

func (s *Source) Schema() []idk.Field {
	s.schemaLock.Lock()
	defer s.schemaLock.Unlock()
	return s.schema
}

func (s *Source) run() {
	defer close(s.records)

	s.expectHeader = len(s.Header) == 0
	if !s.expectHeader {
		var err error
		s.schemaLock.Lock()
		s.schema, err = s.processHeader(s.Header)
		s.schemaLock.Unlock()
		if err != nil {
			s.records <- Record{err: errors.Wrapf(err, "processing given header: %+v", s.Header)}
			return
		}
	}

	for filename := range s.Files {
		s.processFile(filename)
	}
}

func (s *Source) processHeader(header []string) (schema []idk.Field, err error) {
	schema = make([]idk.Field, len(header))
	for i, val := range header {
		schema[i], err = idk.HeaderToField(val, s.Log)
		if err != nil && s.JustDoIt {
			schema[i] = idk.StringField{NameVal: strings.ToLower(val)}
		} else if err != nil {
			return nil, err
		}
	}
	return schema, nil
}

func (s *Source) processFile(name string) {
	s.Log.Printf("processFile: %s", name)

	f, err := openFileOrURL(name)
	if err != nil {
		s.records <- Record{err: errors.Wrapf(err, "opening %s", name)}
		return
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.ReuseRecord = true
	reader.FieldsPerRecord = 0
	var nextErr error
	if s.expectHeader || s.IgnoreHeader {
		header, err := reader.Read()
		if err != nil {
			s.records <- Record{err: errors.Wrapf(err, "reading header from '%s'", name)}
			return
		}
		if s.expectHeader {
			newschema, err := s.processHeader(header)
			if err != nil {
				s.Log.Printf("processHeader error: %v\n", err)
				s.records <- Record{err: errors.Wrapf(err, "processing header from '%s': %+v", name, header)}
				return
			}
			if !reflect.DeepEqual(newschema, s.schema) {
				s.schema = newschema
				nextErr = idk.ErrSchemaChange
			}
		}
	}
	// weird hack to reuse memory rather than allocating eery
	// time. Would probably be better to get rid of the channel and
	// just the next CSV row on demand.
	recs := [2]Record{
		{data: make([]interface{}, len(s.schema)), err: nextErr},
		{data: make([]interface{}, len(s.schema))},
	}
	i := -1
	extraColumnsCount := 0
	row, err := reader.Read()
	for ; err == nil; row, err = reader.Read() {
		i += 1
		for j, val := range row {
			if len(s.schema) <= j {
				if extraColumnsCount == 0 {
					s.Log.Warnf("'%s': ignoring additional column(s) not included in the header specification", name)
				}
				extraColumnsCount++
				break
			}
			recs[i%2].data[j] = val
		}
		s.records <- recs[i%2]
		recs[i%2].err = nil
	}
	if extraColumnsCount > 0 {
		s.Log.Printf("Processing '%s': %d rows have more columns than header specification", name, extraColumnsCount)
	}
	if err != io.EOF {
		s.Log.Printf("ERROR Processing '%s': '%v'. Skipping rest of file.", name, err)
	}
}

func NewSource() *Source {
	return &Source{
		records: make(chan Record, 0), // nolint: gosimple // do not change buffer size!
	}
}

func openFileOrURL(name string) (io.ReadCloser, error) {
	var content io.ReadCloser
	if strings.HasPrefix(name, "http") {
		resp, err := http.Get(name)
		if err != nil {
			return nil, errors.Wrap(err, "getting via http")
		}
		if resp.StatusCode > 299 {
			return nil, errors.Errorf("got status %d via http.Get", resp.StatusCode)
		}
		content = resp.Body
	} else {
		f, err := os.Open(name)
		if err != nil {
			return nil, errors.Wrap(err, "opening file")
		}
		content = f
	}
	return content, nil
}

func (s *Source) Close() error {
	return nil
}
