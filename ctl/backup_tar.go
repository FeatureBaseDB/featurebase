// Copyright 2022 Molecula Corp. All rights reserved.
package ctl

import (
	"archive/tar"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/authn"
	"github.com/featurebasedb/featurebase/v3/buffer"
	"github.com/featurebasedb/featurebase/v3/disco"
	"github.com/featurebasedb/featurebase/v3/encoding/proto"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/server"
	"github.com/pkg/errors"
)

// BackupTarCommand represents a command for backing up a Pilosa node.
type BackupTarCommand struct { // nolint: maligned
	tlsConfig *tls.Config

	// Destination host and port.
	Host string `json:"host"`

	// Optional Index filter
	Index string `json:"index"`

	// Path to write the backup to.
	OutputPath string

	// TempDir location of scratch files
	TempDir string

	// Amount of time after first failed request to continue retrying.
	RetryPeriod time.Duration `json:"retry-period"`

	// Response Header Timeout for HTTP Requests
	HeaderTimeoutStr string
	HeaderTimeout    time.Duration `json:"header-timeout"`

	// Host:port on which to listen for pprof.
	Pprof string `json:"pprof"`

	// Reusable client.
	client *pilosa.InternalClient

	// Standard input/output
	logwriter io.Writer
	logDest   logger.Logger

	TLS server.TLSConfig

	AuthToken string
}

// Logger returns the command's associated Logger to maintain CommandWithTLSSupport interface compatibility
func (cmd *BackupTarCommand) Logger() logger.Logger {
	return cmd.logDest
}

// NewBackupTarCommand returns a new instance of BackupCommand.
func NewBackupTarCommand(logwriter io.Writer) *BackupTarCommand {
	return &BackupTarCommand{
		logwriter:     logwriter,
		logDest:       logger.NewStandardLogger(logwriter),
		RetryPeriod:   time.Minute,
		HeaderTimeout: time.Second * 3,
		Pprof:         "localhost:0",
	}
}

// Run executes the main program execution.
func (cmd *BackupTarCommand) Run(ctx context.Context) (err error) {
	logdest := cmd.Logger()
	// Validate arguments.
	if cmd.OutputPath == "" {
		return fmt.Errorf("%w: -o flag required", UsageError)
	}
	useStdout := cmd.OutputPath == "-"
	if useStdout && cmd.logwriter == os.Stdout {
		logdest = logger.NewStandardLogger(os.Stderr)
	}

	// This was the very first thing in the function, but since logging to stdout causes file corruption
	// if the tarfile is also going to stdout, we need to check that before we can safely send anything
	// to the logger.
	close, err := startProfilingServer(cmd.Pprof, logdest)
	if err != nil {
		return errors.Wrap(err, "starting profiling server")
	}
	defer close()

	if cmd.HeaderTimeoutStr != "" {
		if dur, err := time.ParseDuration(cmd.HeaderTimeoutStr); err != nil {
			return fmt.Errorf("%w: could not parse '%s' as a duration: %v", UsageError, cmd.HeaderTimeoutStr, err)
		} else {
			cmd.HeaderTimeout = dur
		}
	}

	// Parse TLS configuration for node-specific clients.
	tls := cmd.TLSConfiguration()
	if cmd.tlsConfig, err = server.GetTLSConfig(&tls, cmd.Logger()); err != nil {
		return fmt.Errorf("parsing tls config: %w", err)
	}

	// Create a client to the server.
	client, err := commandClient(cmd, pilosa.WithClientRetryPeriod(cmd.RetryPeriod), pilosa.ClientResponseHeaderTimeoutOption(cmd.HeaderTimeout))
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	cmd.client = client

	if cmd.AuthToken != "" {
		ctx = authn.WithAccessToken(ctx, "Bearer "+cmd.AuthToken)
	}

	// Determine the field type in order to correctly handle the input data.
	indexes, err := cmd.client.Schema(ctx)
	if err != nil {
		return fmt.Errorf("getting schema: %w", err)
	}
	if cmd.Index != "" {
		for _, idx := range indexes {
			if idx.Name == cmd.Index {
				indexes = make([]*pilosa.IndexInfo, 0)
				indexes = append(indexes, idx)
				break
			}
		}
		if len(indexes) <= 0 {
			return fmt.Errorf("index not found to back up")
		}
	}
	schema := &pilosa.Schema{Indexes: indexes}

	// Create output file in temporary location, or send to stdout if a dash is specified.
	var w io.Writer
	if useStdout {
		w = os.Stdout
		// if writing tarfile to stdout, the logs can't also go there or the file ends up corrupt
		// redirect to stderr and log a message there to avoid this
		// commented out for testing
		//if dest := logger.Logger(); dest.Writer() == os.Stdout {
		//	dest.SetOutput(os.Stderr)
		//	logger.Printf("redirected logs to stderr to avoid file corruption")
		//}
	} else {
		f, err := os.Create(cmd.OutputPath + ".tmp")
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}

	// Open a tar writer to the temporary file.
	tw := tar.NewWriter(w)
	defer tw.Close()

	// Backup schema.
	if err := cmd.backupTarSchema(ctx, tw, schema); err != nil {
		return fmt.Errorf("cannot back up schema: %w", err)
	} else if err := cmd.backupTarIDAllocData(ctx, tw); err != nil {
		return fmt.Errorf("cannot back up id alloc data: %w", err)
	}

	// Backup data for each index.
	for _, ii := range schema.Indexes {
		if err := cmd.backupTarIndex(ctx, tw, ii); err != nil {
			return err
		}
	}

	// Close archive.
	if err := tw.Close(); err != nil {
		return err
	}

	// Move data file to final location.
	if !useStdout {
		logdest.Printf("writing backup: %s", cmd.OutputPath)
		if err := os.Rename(cmd.OutputPath+".tmp", cmd.OutputPath); err != nil {
			return err
		}
	}

	return nil
}

// backupTarSchema writes the schema to the archive.
func (cmd *BackupTarCommand) backupTarSchema(ctx context.Context, tw *tar.Writer, schema *pilosa.Schema) error {
	logger := cmd.Logger()
	logger.Printf("backing up schema")

	buf, err := json.MarshalIndent(schema, "", "\t")
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}

	// Build header & copy data to archive.
	if err = tw.WriteHeader(&tar.Header{
		Name:    "schema",
		Mode:    0o666,
		Size:    int64(len(buf)),
		ModTime: time.Now(),
	}); err != nil {
		return err
	} else if _, err := tw.Write(buf); err != nil {
		return fmt.Errorf("copying schema to archive: %w", err)
	}

	return nil
}

func (cmd *BackupTarCommand) backupTarIDAllocData(ctx context.Context, tw *tar.Writer) error {
	logger := cmd.Logger()
	logger.Printf("backing up id alloc data")

	rc, err := cmd.client.IDAllocDataReader(ctx)
	if err != nil {
		return fmt.Errorf("fetching id alloc data reader: %w", err)
	}
	defer rc.Close()

	return writeToTar(tw, "idalloc", rc, cmd.TempDir)
}

// backupTarIndex backs up all shards for a given index.
func (cmd *BackupTarCommand) backupTarIndex(ctx context.Context, tw *tar.Writer, ii *pilosa.IndexInfo) error {
	logger := cmd.Logger()
	logger.Printf("backing up index: %q", ii.Name)

	shards, err := cmd.client.AvailableShards(ctx, ii.Name)
	if err != nil {
		return fmt.Errorf("cannot find available shards for index %q: %w", ii.Name, err)
	}

	// Back up all bitmap data for the index.
	for _, shard := range shards {
		if err := cmd.backupTarShard(ctx, tw, ii.Name, shard); err != nil {
			return fmt.Errorf("cannot backup shard %d on index %q: %w", shard, ii.Name, err)
		}
	}

	if ii.Options.Keys {
		// Back up translation data after bitmap data so we ensurean translate all data.
		if err := cmd.backupTarIndexTranslateData(ctx, tw, ii.Name); err != nil {
			return err
		}
	}

	// Back up field translation data.
	for _, fi := range ii.Fields {
		if !fi.Options.Keys {
			continue
		}
		if err := cmd.backupTarFieldTranslateData(ctx, tw, ii.Name, fi.Name); err != nil {
			return fmt.Errorf("cannot backup field translation data for field %q on index %q: %w", fi.Name, ii.Name, err)
		}
	}

	return nil
}

// backupTarShard backs up a single shard from a single index.
func (cmd *BackupTarCommand) backupTarShard(ctx context.Context, tw *tar.Writer, indexName string, shard uint64) (err error) {
	nodes, err := cmd.client.FragmentNodes(ctx, indexName, shard)
	if err != nil {
		return fmt.Errorf("cannot determine fragment nodes: %w", err)
	} else if len(nodes) == 0 {
		return fmt.Errorf("no nodes available")
	}

	for _, node := range nodes {
		if e := cmd.backupTarShardNode(ctx, tw, indexName, shard, node); e == nil {
			break
		} else if err == nil {
			err = e // save first error, try next node
		}
	}

	for _, node := range nodes {
		if e := cmd.backupTarShardDataframe(ctx, tw, indexName, shard, node); e == nil {
			break
		} else if err == nil {
			err = e // save first error, try next node
		}
	}
	return err
}

// backupTarShardNode backs up a single shard from a single index on a specific node.
func (cmd *BackupTarCommand) backupTarShardNode(ctx context.Context, tw *tar.Writer, indexName string, shard uint64, node *disco.Node) error {
	logger := cmd.Logger()
	logger.Printf("backing up shard: index=%q id=%d", indexName, shard)

	filename := path.Join("indexes", indexName, "shards", fmt.Sprintf("%04d", shard))

	client := pilosa.NewInternalClientFromURI(&node.URI,
		pilosa.GetHTTPClient(cmd.tlsConfig, pilosa.ClientResponseHeaderTimeoutOption(cmd.HeaderTimeout)),
		pilosa.WithClientRetryPeriod(cmd.RetryPeriod),
		pilosa.WithSerializer(proto.Serializer{}))
	rc, err := client.ShardReader(ctx, indexName, shard)
	if err != nil {
		return fmt.Errorf("fetching shard reader: %w", err)
	}
	defer rc.Close()
	return writeToTar(tw, filename, rc, cmd.TempDir)
}

func (cmd *BackupTarCommand) backupTarShardDataframe(ctx context.Context, tw *tar.Writer, indexName string, shard uint64, node *disco.Node) error {
	logger := cmd.Logger()
	logger.Printf("backing up dataframe shard: index=%q shard=%d", indexName, shard)

	client := pilosa.NewInternalClientFromURI(&node.URI,
		pilosa.GetHTTPClient(cmd.tlsConfig, pilosa.ClientResponseHeaderTimeoutOption(cmd.HeaderTimeout)),
	)

	resp, err := client.GetDataframeShard(ctx, indexName, shard)
	// no error if doesn't exist
	if err != nil {
		return fmt.Errorf("getting dataframe: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		// no error if not present server maynot have it turned on
		return nil
	}

	filename := filepath.Join("indexes", indexName, "dataframe", fmt.Sprintf("%04d", shard))
	return writeToTar(tw, filename, resp.Body, cmd.TempDir)
}

func (cmd *BackupTarCommand) backupTarIndexTranslateData(ctx context.Context, tw *tar.Writer, name string) error {
	// TODO: Fetch holder partition count.
	partitionN := disco.DefaultPartitionN
	for partitionID := 0; partitionID < partitionN; partitionID++ {
		if err := cmd.backupTarIndexPartitionTranslateData(ctx, tw, name, partitionID); err != nil {
			return fmt.Errorf("cannot backup index translation data for partition %d on %q: %w", partitionID, name, err)
		}
	}
	return nil
}

func (cmd *BackupTarCommand) backupTarIndexPartitionTranslateData(ctx context.Context, tw *tar.Writer, name string, partitionID int) error {
	logger := cmd.Logger()
	logger.Printf("backing up index translation data: %s/%d", name, partitionID)

	rc, err := cmd.client.IndexTranslateDataReader(ctx, name, partitionID)
	if err == pilosa.ErrTranslateStoreNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("fetching translate data reader: %w", err)
	}
	defer rc.Close()

	return writeToTar(tw, path.Join("indexes", name, "translate", fmt.Sprintf("%04d", partitionID)), rc, cmd.TempDir)
}

func (cmd *BackupTarCommand) backupTarFieldTranslateData(ctx context.Context, tw *tar.Writer, indexName, fieldName string) error {
	logger := cmd.Logger()
	logger.Printf("backing up field translation data: %s/%s", indexName, fieldName)

	rc, err := cmd.client.FieldTranslateDataReader(ctx, indexName, fieldName)
	if err == pilosa.ErrTranslateStoreNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("fetching translate data reader: %w", err)
	}
	defer rc.Close()
	return writeToTar(tw, path.Join("indexes", indexName, "fields", fieldName, "translate"), rc, cmd.TempDir)
}

func (cmd *BackupTarCommand) TLSHost() string { return cmd.Host }

func (cmd *BackupTarCommand) TLSConfiguration() server.TLSConfig { return cmd.TLS }

func writeToTar(tw *tar.Writer, entryName string, rc io.Reader, tmpDir string) error {
	spillFile, err := os.CreateTemp("", "spill")
	if err != nil {
		return fmt.Errorf("creating temp file : %w", err)
	}
	defer func() {
		spillFile.Close()
		os.Remove(spillFile.Name())
	}()
	mb512 := 2 << 29
	buf := buffer.NewFileBuffer(mb512, tmpDir)
	defer buf.Close()

	n, err := io.Copy(buf, rc)
	// Read to buffer to determine size.
	if err != nil {
		return fmt.Errorf("copying translate data to memory: %w", err)
	}

	// Build header & copy data to archive.
	if err = tw.WriteHeader(&tar.Header{
		Name:    entryName,
		Mode:    0o666,
		Size:    n,
		ModTime: time.Now(),
	}); err != nil {
		return err
	} else if _, err := io.Copy(tw, buf); err != nil {
		return fmt.Errorf("copying translate data to archive: %w", err)
	}
	return nil
}
