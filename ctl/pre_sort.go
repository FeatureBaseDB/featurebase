package ctl

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type PreSortCommand struct {
	// Optional Index filter
	File string `json:"file"`

	Type string `json:"type"`

	Table string `json:"table"`

	PrimaryKeyFields    []string `json:"primary-key-fields"`
	PrimaryKeySeparator string

	PartitionN int `json:"partition-n"`

	JobSize    int `json:"job-size"`
	NumWorkers int `json:"num-workers"`

	// Path to write sorted files to
	OutputDir string

	// Standard input/output
	logDest logger.Logger

	outputLocks    []sync.Mutex
	outputFiles    map[int]*os.File
	outputEncoders map[int]*json.Encoder
	outputWriters  map[int]*csv.Writer
}

// NewPreSortCommand returns a new instance of PreSortCommand.
func NewPreSortCommand(logdest logger.Logger) *PreSortCommand {
	return &PreSortCommand{
		logDest:             logdest,
		Type:                "ndjson",
		OutputDir:           "presorted_files",
		PartitionN:          256,
		JobSize:             1000,
		NumWorkers:          4,
		PrimaryKeySeparator: "|",
		outputFiles:         make(map[int]*os.File),
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
	cmd.outputLocks = make([]sync.Mutex, cmd.PartitionN)
	filelist := []string{}
	walkfunc := func(path string, d fs.DirEntry, err error) error {
		if !d.IsDir() {
			filelist = append(filelist, path)
		}
		return nil
	}
	err := filepath.WalkDir(cmd.File, walkfunc)
	if err != nil {
		return errors.Wrapf(err, "walking %s", cmd.File)
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
			_ = f.Sync()
			_ = f.Close()
		}
	}()

	linech := make(chan [][]byte, 16)
	var eg *errgroup.Group
	switch cmd.Type {
	case "ndjson":
		eg = cmd.setupNDJSON(linech)
	case "csv":
		// CSV ends here
		return errors.Wrap(cmd.RunCSV(filelist), "running CSV command")
	default:
		return errors.Errorf("unsupported type %s, try ndjson or csv", cmd.Type)
	}

	// The rest of this is just for NDJSON... CSV doesn't currently
	// support multiple workers. It needs to do the file reading
	// differently in order to handle potential newlines within
	// fields.
	for _, fname := range filelist {
		f, err := os.Open(fname)
		if err != nil {
			return errors.Wrapf(err, "opening %s", fname)
		}

		sc := bufio.NewScanner(f)
	batch:
		for {
			lines := make([][]byte, 0, cmd.JobSize)
			for i := 0; i < cmd.JobSize; i++ {
				if !sc.Scan() {
					linech <- lines
					break batch
				}
				line := sc.Bytes()
				// must copy line as scanner will reuse the buffer it's given us
				lc := make([]byte, len(line))
				copy(lc, line)
				lines = append(lines, lc)
			}
			linech <- lines
		}
		if err := sc.Err(); err != nil {
			return errors.Wrap(err, "scanning file")
		}
	}

	close(linech)
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func (cmd *PreSortCommand) setupNDJSON(linech chan [][]byte) *errgroup.Group {
	cmd.outputEncoders = make(map[int]*json.Encoder)
	for i, f := range cmd.outputFiles {
		cmd.outputEncoders[i] = json.NewEncoder(f)
		cmd.outputEncoders[i].SetIndent("", "")
		cmd.outputEncoders[i].SetEscapeHTML(false)
	}

	// start workers
	eg := &errgroup.Group{}
	for i := 0; i < cmd.NumWorkers; i++ {
		eg.Go(func() error {
			return cmd.ndjsonWorker(linech)
		})
	}
	return eg
}

func (cmd *PreSortCommand) ndjsonWorker(linech chan [][]byte) error {
	for lines := range linech {
		rec := map[string]interface{}{}
		for _, line := range lines {
			err := json.Unmarshal(line, &rec)
			if err != nil {
				return errors.Wrapf(err, "unmarshaling '%s'", line)
			}
			partition, err := cmd.ndjsonPartition(rec)
			if err != nil {
				return err
			}
			if err := cmd.outputNDJSON(partition, rec); err != nil {
				return errors.Wrap(err, "outputting ndjson")
			}
			for k := range rec {
				delete(rec, k)
			}
		}
	}
	return nil
}

func (cmd *PreSortCommand) outputNDJSON(partition int, record map[string]interface{}) error {
	cmd.outputLocks[partition].Lock()
	defer cmd.outputLocks[partition].Unlock()

	return cmd.outputEncoders[partition].Encode(record)
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
			h.Write([]byte(cmd.PrimaryKeySeparator))
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
		h.Write([]byte(rec[pos]))
		if i < len(cmd.PrimaryKeyFields)-1 {
			h.Write([]byte(cmd.PrimaryKeySeparator))
		}
	}
	return int(h.Sum64() % uint64(cmd.PartitionN)), nil
}
