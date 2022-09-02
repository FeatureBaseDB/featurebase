package csv

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"sync"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/pkg/errors"
)

type Main struct {
	idk.Main     `flag:"!embed"`
	Files        []string `help:"List of files, URLs, or directories to ingest."`
	Header       []string `help:"Optional header. If not passed, first line of each file is used."`
	IgnoreHeader bool     `short:"g" help:"Ignore header in file and use configured header. You *must* configure a header."`
	JustDoIt     bool     `short:"j" help:"Any header field not in the appropriate format, just downcase, use it as the name and process the value as a String/set field"`

	files   chan string
	visited map[string]struct{}
}

func NewMain() *Main {
	m := &Main{
		Main:    *idk.NewMain(),
		files:   make(chan string),
		visited: make(map[string]struct{}),
	}
	m.Main.Namespace = "ingester_csv"

	once := &sync.Once{}
	m.NewSource = func() (idk.Source, error) {
		if len(m.Files) == 0 {
			return nil, errors.New("must provide at least one file or directory with --files")
		}
		once.Do(func() { go m.streamFileNames() })

		source := NewSource()
		source.Files = m.files
		source.Header = m.Header
		source.Log = m.Main.Log()
		source.once = &sync.Once{}
		source.JustDoIt = m.JustDoIt
		source.IgnoreHeader = m.IgnoreHeader
		return source, nil
	}
	return m
}

// ValidateHeaders is used during --dry-run to check that all headers provided at CLI or in
// CSV files can be parsed properly.
// Also returns the field structs as it would be parsed in a normal run. In the case of
// multiple CSV files, only returns the fields for the last file.
func (m *Main) ValidateHeaders() ([]idk.Field, error) {
	var err error
	fields := make([]idk.Field, 0)
	if len(m.Header) > 0 {
		fields, err = m.validateHeader(m.Header)
		if err != nil {
			return fields, errors.Wrap(err, "validating header")
		}
	} else {
		for _, f := range m.Files {
			f, err := openFileOrURL(f)
			if err != nil {
				return fields, errors.Wrapf(err, "opening file (%s)", f)
			}
			defer f.Close()
			reader := csv.NewReader(f)
			header, err := reader.Read()
			if err != nil {
				return fields, errors.Wrapf(err, "reading CSV file (%s)", f)
			}
			fields, err = m.validateHeader(header)
			if err != nil {
				return fields, errors.Wrap(err, "validating header")
			}
		}
	}
	return fields, nil
}

func (m *Main) validateHeader(h []string) ([]idk.Field, error) {
	fields := make([]idk.Field, 0)
	for _, val := range h {
		field, err := idk.HeaderToField(val, m.Log())
		if err != nil {
			return fields, errors.Wrapf(err, "invalid header (%v)", val)
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func (m *Main) streamFileNames() {
	defer close(m.files)

	for _, filename := range m.Files {
		if err := filepath.Walk(filename, m.traverse); err != nil {
			m.Log().Printf("filepath.Walk(%s) %v", filename, err)
			continue
		}
	}
}

func (m *Main) traverse(path string, fi os.FileInfo, err error) error {
	if err != nil {
		m.Log().Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
		return err
	}

	switch mode := fi.Mode(); {
	case mode.IsRegular():
		if _, ok := m.visited[path]; !ok {
			m.visited[path] = struct{}{}
			m.files <- path
		}

	case mode.IsDir():
		if _, ok := m.visited[path]; !ok {
			m.visited[path] = struct{}{}
		}

	case mode&os.ModeSymlink != 0:
		lnk, err := filepath.EvalSymlinks(path)
		if err != nil {
			return err
		}
		if _, ok := m.visited[lnk]; !ok {
			if err := filepath.Walk(lnk, m.traverse); err != nil {
				return err
			}
		}
	}

	return nil
}
