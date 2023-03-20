// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/client"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
)

var (
	mask     = uint64(pilosa.ShardWidth - 1)
	Sentinal = uint64(math.MaxUint64)
)

func init() {
	gob.Register(arrow.PrimitiveTypes.Int64)
	gob.Register(arrow.PrimitiveTypes.Float64)
	gob.Register(arrow.BinaryTypes.String)
}

// TODO(rdp): add refresh token to this as well

// DataframeCsvLoaderCommand is used to load a dataframe from CSV.
type DataframeCsvLoaderCommand struct {
	tlsConfig *tls.Config

	Host string

	Index string

	// Filepath to the csv file
	Path string

	// max line length of csv file
	MaxCapacity int

	// Batch Size
	BatchSize int

	// Host:port on which to listen for pprof.
	Pprof string `json:"pprof"`

	TLS           idk.TLSConfig
	AuthToken     string            `flag:"auth-token" help:"Authentication Token for FeatureBase"`
	SchemaManager idk.SchemaManager `flag:"-"`

	// Reusable client.
	client          *client.Client
	index           *client.Index
	needTranslation bool

	// Standard input/output
	log logger.Logger
}

// Logger returns the command's associated Logger to maintain CommandWithTLSSupport interface compatibility
func (cmd *DataframeCsvLoaderCommand) Logger() logger.Logger {
	return cmd.log
}

// NewDataframeCsvLoaderCommand returns a new instance of DataframeCsvLoaderCommand.
func NewDataframeCsvLoaderCommand(logdest logger.Logger) *DataframeCsvLoaderCommand {
	return &DataframeCsvLoaderCommand{
		log:   logdest,
		Pprof: "localhost:0",
	}
}

func (cmd *DataframeCsvLoaderCommand) setupClient() (*tls.Config, error) {
	var tlsConfig *tls.Config
	var err error
	opts := []client.ClientOption{}
	if cmd.TLS.CertificatePath != "" {
		tlsConfig, err = idk.GetTLSConfig(&cmd.TLS, cmd.log)
		if err != nil {
			return nil, errors.Wrap(err, "getting TLS config")
		}
		opts = append(opts, client.OptClientTLSConfig(tlsConfig))
	} else {
		opts = append(opts,
			client.OptClientRetries(2),
			client.OptClientTotalPoolSize(1000),
			client.OptClientPoolSizePerRoute(400),
		)
	}
	cmd.client, err = client.NewClient([]string{cmd.Host}, opts...)
	if err != nil {
		return nil, err
	}
	cmd.client.AuthToken = cmd.AuthToken
	if err != nil {
		return nil, errors.Wrap(err, "getting featurebase client")
	}
	return tlsConfig, nil
}

func (cmd *DataframeCsvLoaderCommand) Setup() (err error) {
	// setup logging
	cmd.log = logger.NewStandardLogger(os.Stderr)
	if cmd.Pprof != "" {
		go func() {
			runtime.SetBlockProfileRate(10000000) // 1 sample per 10 ms
			runtime.SetMutexProfileFraction(100)  // 1% sampling
			cmd.log.Printf("Listening for /debug/pprof/ and /debug/fgprof on '%s'", cmd.Pprof)
			cmd.log.Printf("%v", http.ListenAndServe(cmd.Pprof, nil))
		}()
	}
	// set up Pilosa client
	_, err = cmd.setupClient()
	if err != nil {
		return errors.Wrap(err, "setting up client")
	}

	if cmd.AuthToken != "" {
		cmd.AuthToken = "Bearer " + cmd.AuthToken // Gets added to context
	}

	return nil
}

// Run executes the dataload.
func (cmd *DataframeCsvLoaderCommand) Run(ctx context.Context) (err error) {
	err = cmd.Setup()
	if err != nil {
		return err
	}
	logger := cmd.Logger()
	close, err := startProfilingServer(cmd.Pprof, logger)
	if err != nil {
		return errors.Wrap(err, "starting profiling server")
	}
	defer close()

	// Validate arguments.
	if cmd.Path == "" {
		return fmt.Errorf("%w: --csv flag required", ErrUsage)
	}

	readFile, err := os.Open(cmd.Path)
	if err != nil {
		return err
	}
	fields := make([]arrow.Field, 0)
	fields = append(fields, arrow.Field{Name: "_ID", Type: arrow.PrimitiveTypes.Int64})
	fileScanner := bufio.NewScanner(readFile) // TODO(twg) 2023/01/11 need to convert to the go CSV reader for more robust string support
	fileScanner.Split(bufio.ScanLines)
	// need for really long csv lines
	var buf []byte
	if cmd.MaxCapacity > 0 {
		buf = make([]byte, cmd.MaxCapacity)
		fileScanner.Buffer(buf, cmd.MaxCapacity)
	}
	total := 0
	if fileScanner.Scan() {
		total++
		t := fileScanner.Text()
		p := strings.Split(t, ",")
		for _, col := range p[1:] {
			col = strings.TrimSpace(col)
			cmd.Logger().Infof("checking %v", col)
			name := col[:strings.LastIndex(col, "__")]
			cmd.Logger().Infof("name:%v", name)
			if strings.HasSuffix(col, "__I") {
				fields = append(fields, arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64})
			} else if strings.HasSuffix(col, "__F") {
				fields = append(fields, arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64})
			} else if strings.HasSuffix(col, "__S") {
				fields = append(fields, arrow.Field{Name: name, Type: arrow.BinaryTypes.String})
			} else {
				return errors.New("invalid format for type")
			}
		}

	} else {
		return errors.Wrap(fileScanner.Err(), "No header")
	}
	schema, err := cmd.client.Schema()
	if err != nil {
		return err
	}
	idx := schema.Index(cmd.Index)
	if idx.Opts().Keys() {
		cmd.needTranslation = true
		cmd.index = idx
	}

	arrowSchema := arrow.NewSchema(fields, nil)
	keys := make([]string, 0)
	lookup := make(map[string]uint64)
	if cmd.needTranslation {

		for fileScanner.Scan() {
			t := fileScanner.Text()
			r := t[:strings.Index(t, ",")]
			_, ok := lookup[r]
			if !ok {
				keys = append(keys, r)
				lookup[r] = Sentinal
			}

		}
		cmd.Logger().Infof("Translate Keys %d", total)
		ids, err := cmd.client.CreateIndexKeys(cmd.index, keys...)
		if err != nil {
			return err
		}
		lookup = ids
	}
	sharder := &Sharder{
		shards: make(map[uint64]*ShardDiff),
		schema: arrowSchema,
		index:  cmd.Index,
		log:    cmd.log,
	}
	readFile.Seek(0, io.SeekStart)
	fileScanner = bufio.NewScanner(readFile)
	if cmd.MaxCapacity > 0 {
		fileScanner.Buffer(buf, cmd.MaxCapacity)
	}
	fileScanner.Split(bufio.ScanLines)
	fileScanner.Scan() // skip the header
	cmd.Logger().Infof("Build the dataframe input package in memory")
	id := uint64(0)
	recordCounter := 0
	for fileScanner.Scan() {
		recordCounter++
		records := strings.Split(fileScanner.Text(), ",")
		if cmd.needTranslation {
			id = lookup[records[0]]
		} else {
			id, err = strconv.ParseUint(records[0], 10, 64)
			if err != nil {
				return err
			}
		}
		shard := id / pilosa.ShardWidth
		shardFile, err := sharder.GetShard(shard)
		if err != nil {
			return err
		}
		shardRow := int64(id & mask)
		shardFile.SetRow(shardRow)
		for i, rec := range records {
			if i == 0 {
				shardFile.SetIntValue(i, shardRow, int64(id))
			} else {
				rec = strings.TrimSpace(rec)
				switch arrowSchema.Field(i).Type {
				case arrow.PrimitiveTypes.Int64:
					val, err := strconv.ParseInt(rec, 10, 64)
					if err != nil {
						shardFile.SetIntValue(i, shardRow, 0)
						continue
					}
					shardFile.SetIntValue(i, shardRow, val)
				case arrow.PrimitiveTypes.Float64:
					val, err := strconv.ParseFloat(rec, 64)
					if err != nil {
						shardFile.SetFloatValue(i, shardRow, 0)
						continue
					}
					shardFile.SetFloatValue(i, shardRow, val)
				case arrow.BinaryTypes.String:
					shardFile.SetStringValue(i, shardRow, rec)
				default:
					return errors.New("unhandled arrow type type")
				}
			}
		}
		if recordCounter > cmd.BatchSize {
			cmd.Logger().Infof("sending package to featurebase")
			err = sharder.Store(arrowSchema, cmd.client)
			if err != nil {
				return err
			}
			sharder.Reset()
			recordCounter = 0
		}
	}
	if recordCounter > 0 {
		err = sharder.Store(arrowSchema, cmd.client)
		if err != nil {
			return err
		}
	}
	return nil
}

type pair struct {
	col int
	row uint64
}

type ShardDiff struct {
	columns []interface{}
	rows    []int64
	null    map[pair]struct{}
	shard   uint64
	// Standard input/output
	log logger.Logger
}

func NewShardDiff(shard uint64, log logger.Logger) (*ShardDiff, error) {
	return &ShardDiff{shard: shard, log: log}, nil
}

type Number interface {
	int64 | float64
}

func (s *ShardDiff) SetIntValue(col int, row int64, val int64) {
	slice := s.columns[col].([]int64)
	s.columns[col] = append(slice, val)
}

func (s *ShardDiff) SetFloatValue(col int, row int64, val float64) {
	slice := s.columns[col].([]float64)
	s.columns[col] = append(slice, val)
}

func (s *ShardDiff) SetStringValue(col int, row int64, val string) {
	slice := s.columns[col].([]string)
	s.columns[col] = append(slice, val)
}

func (s *ShardDiff) SetNulll(col int, row uint64) {
	s.null[pair{col: col, row: row}] = struct{}{}
}

func (s *ShardDiff) SetRow(row int64) {
	s.rows = append(s.rows, row)
}

func (s *ShardDiff) Setup(schema *arrow.Schema) {
	for _, f := range schema.Fields() {
		switch f.Type {
		case arrow.PrimitiveTypes.Int64:
			s.columns = append(s.columns, make([]int64, 0))
		case arrow.PrimitiveTypes.Float64:
			s.columns = append(s.columns, make([]float64, 0))
		case arrow.BinaryTypes.String:
			s.columns = append(s.columns, make([]string, 0))
		}
	}
}

func makeSimpleSchema(a *arrow.Schema) []pilosa.NameType {
	nt := make([]pilosa.NameType, len(a.Fields()))

	for i := 0; i < len(a.Fields()); i++ {
		f := a.Field(i)
		nt[i] = pilosa.NameType{Name: f.Name, DataType: f.Type}
	}
	return nt
}

func (s *ShardDiff) Store(index string, schema *arrow.Schema, fb *client.Client) error {
	s.log.Infof("dataframe for shard %v:%v:", index, s.shard)
	request := &pilosa.ChangesetRequest{}
	request.Columns = s.columns
	request.ShardIds = s.rows
	request.SimpleSchema = makeSimpleSchema(schema)
	_, err := fb.ApplyDataframeChangeset(index, request, s.shard)
	return err
}

func (s *ShardDiff) IsValid(col int, row int) bool {
	_, ok := s.null[pair{col: col, row: uint64(row)}]
	return !ok
}

type Sharder struct {
	shards map[uint64]*ShardDiff
	index  string
	schema *arrow.Schema
	// Standard input/output
	log logger.Logger
}

func (s *Sharder) Reset() {
	s.shards = make(map[uint64]*ShardDiff)
}

func (s *Sharder) GetShard(shard uint64) (*ShardDiff, error) {
	f, ok := s.shards[shard]
	if ok {
		return f, nil
	}
	f, err := NewShardDiff(shard, s.log)
	f.Setup(s.schema)
	if err != nil {
		return nil, err
	}
	s.shards[shard] = f
	return f, nil
}

func (s *Sharder) Store(schema *arrow.Schema, client *client.Client) error {
	for _, f := range s.shards {
		err := f.Store(s.index, schema, client)
		if err != nil {
			return err
		}
	}
	return nil
}
