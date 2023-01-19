package ctl

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
)

type PreSortCommand struct {
	// Optional Index filter
	File string `json:"file"`

	Type string `json:"type"`

	Table string `json:"table"`

	PrimaryKeyFields []string `json:"primary-key-fields"`

	PartitionN int `json:"partition-n"`

	// Path to write sorted files to
	OutputDir string

	// Standard input/output
	logDest logger.Logger

	outputFiles map[int]*os.File
}

// NewPreSortCommand returns a new instance of PreSortCommand.
func NewPreSortCommand(logdest logger.Logger) *PreSortCommand {
	return &PreSortCommand{
		logDest:     logdest,
		Type:        "ndjson",
		OutputDir:   "presorted_files",
		PartitionN:  256,
		outputFiles: make(map[int]*os.File),
	}
}

// Run executes the main program execution.
func (cmd *PreSortCommand) Run(ctx context.Context) error {
	if cmd.File == "" {
		return errors.New("must set a file or directory to read from")
	}
	if len(cmd.PrimaryKeyFields) == 0 {
		return errors.New("must define primary-key-fields")
	}
	if cmd.Table == "" {
		return errors.New("must set a table name")
	}
	filelist := []string{}
	walkfunc := func(path string, d fs.DirEntry, err error) error {
		if !d.IsDir() {
			filelist = append(filelist, path)
		}
		return nil
	}
	err := filepath.WalkDir(cmd.File, walkfunc)
	if err != nil {
		return errors.Wrapf(err, "wakling %s", cmd.File)
	}
	if len(filelist) > 3 {
		cmd.logDest.Printf("%d files: %v...", len(filelist), filelist[:3])
	} else {
		cmd.logDest.Printf("Files: %v", filelist)
	}

	if err := os.MkdirAll(cmd.OutputDir, 0755); err != nil {
		return errors.Wrapf(err, "creating output directory: '%s'", cmd.OutputDir)
	}

	for i := 0; i < cmd.PartitionN; i++ {
		name := filepath.Join(cmd.OutputDir, fmt.Sprintf("output_%d", i))
		f, err := os.Create(name)
		if err != nil {
			return errors.Wrapf(err, "couldn't open output file %s", name)
		}
		cmd.outputFiles[i] = f
	}
	defer func() {
		for _, f := range cmd.outputFiles {
			_ = f.Close()
		}
	}()

	switch cmd.Type {
	case "ndjson":
		if err := cmd.RunNDJSON(filelist); err != nil {
			return errors.Wrap(err, "running ndjson")
		}
	case "csv":
		if err := cmd.RunCSV(filelist); err != nil {
			return errors.Wrap(err, "running csv")
		}
	default:
		return errors.Errorf("unsupported type %s, try ndjson or csv", cmd.Type)
	}

	return nil
}

func (cmd *PreSortCommand) RunNDJSON(filelist []string) error {
	outputEncoders := make(map[int]*json.Encoder)
	for i, f := range cmd.outputFiles {
		outputEncoders[i] = json.NewEncoder(f)
		outputEncoders[i].SetIndent("", "")
		outputEncoders[i].SetEscapeHTML(false)
	}

	for _, fname := range filelist {
		f, err := os.Open(fname)
		if err != nil {
			cmd.logDest.Warnf("Could not open %s, skipping", fname)
		}
		dec := json.NewDecoder(f)
		rec := map[string]interface{}{}
		for err = dec.Decode(&rec); err == nil; err = dec.Decode(&rec) {
			partition, err := cmd.ndjsonPartition(rec)
			if err != nil {
				return err
			}
			err = outputEncoders[partition].Encode(rec)
			if err != nil {
				return errors.Wrapf(err, "writing record to partition %d", partition)
			}
		}
		if err != io.EOF && err != nil {
			return errors.Wrapf(err, "error attempting to decode json from %s", fname)
		}
	}
	return nil
}

func (cmd *PreSortCommand) ndjsonPartition(rec map[string]interface{}) (int, error) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(cmd.Table))

	for i, name := range cmd.PrimaryKeyFields {
		val, ok := rec[name]
		if !ok {
			return 0, errors.Errorf("couldn't find primary key part '%s' in record: %+v", name, rec)
		}
		h.Write([]byte(toString(val)))
		if i < len(cmd.PrimaryKeyFields)-1 {
			h.Write([]byte{'|'})
		}
	}
	return int(h.Sum64() % uint64(cmd.PartitionN)), nil
}

func (cmd *PreSortCommand) RunCSV(filelist []string) error {
	outputWriters := make(map[int]*csv.Writer)
	for i, f := range cmd.outputFiles {
		outputWriters[i] = csv.NewWriter(f)
	}

	for _, fname := range filelist {
		f, err := os.Open(fname)
		if err != nil {
			cmd.logDest.Warnf("Could not open %s, skipping", fname)
		}
		reader := csv.NewReader(f)
		reader.ReuseRecord = true
		rec, err := reader.Read()
		if err != nil {
			return errors.Wrap(err, "")
		}
		header := make(map[string]int)
		for i, key := range rec {
			header[key] = i
		}
		for rec, err = reader.Read(); err == nil; rec, err = reader.Read() {
			partition, err := cmd.csvPartition(header, rec)
			if err != nil {
				return err
			}
			err = outputWriters[partition].Write(rec)
			if err != nil {
				return errors.Wrapf(err, "writing record to partition %d", partition)
			}
		}
		if err != io.EOF && err != nil {
			return errors.Wrapf(err, "error attempting to decode json from %s", fname)
		}
	}

	for _, w := range outputWriters {
		w.Flush()
	}
	return nil
}

func (cmd *PreSortCommand) csvPartition(header map[string]int, rec []string) (int, error) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(cmd.Table))

	for i, name := range cmd.PrimaryKeyFields {
		pos, ok := header[name]
		if !ok {
			return 0, errors.Errorf("couldn't find primary key part '%s' in header: %+v", name, header)
		}
		val := rec[pos]
		h.Write([]byte(toString(val)))
		if i < len(cmd.PrimaryKeyFields)-1 {
			h.Write([]byte{'|'})
		}
	}
	return int(h.Sum64() % uint64(cmd.PartitionN)), nil
}

func toString(val interface{}) string {
	switch valt := val.(type) {
	case string:
		return valt
	case float64, int, int64, uint64, float32, uint:
		return fmt.Sprintf("%d", valt)
	default:
		return fmt.Sprintf("%v", valt)
	}
}

func (cmd *PreSortCommand) partition(key string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(cmd.Table))
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() % uint64(cmd.PartitionN))
}

func partition(key string) int {
	indexName := "karambit"
	h := fnv.New64a()
	_, _ = h.Write([]byte(indexName))
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() % uint64(256))
}
