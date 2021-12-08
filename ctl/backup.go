// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/http"
	"github.com/molecula/featurebase/v2/server"
	"github.com/molecula/featurebase/v2/topology"
	"golang.org/x/sync/errgroup"
)

// BackupCommand represents a command for backing up a FeatureBase node.
type BackupCommand struct { // nolint: maligned
	tlsConfig *tls.Config

	// Destination host and port.
	Host string `json:"host"`

	// Optional Index filter
	Index string `json:"index"`

	// Path to write the backup to.
	OutputDir string

	// If true, skips file sync.
	NoSync bool

	// Number of concurrent backup goroutines running at a time.
	Concurrency int

	// Reusable client.
	client pilosa.InternalClient

	// Standard input/output
	*pilosa.CmdIO

	TLS server.TLSConfig
}

// NewBackupCommand returns a new instance of BackupCommand.
func NewBackupCommand(stdin io.Reader, stdout, stderr io.Writer) *BackupCommand {
	return &BackupCommand{
		CmdIO:       pilosa.NewCmdIO(stdin, stdout, stderr),
		Concurrency: 1,
	}
}

// Run executes the main program execution.
func (cmd *BackupCommand) Run(ctx context.Context) (err error) {
	// Validate arguments.
	if cmd.OutputDir == "" {
		return fmt.Errorf("-o flag required")
	} else if cmd.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be at least one")
	}

	// Parse TLS configuration for node-specific clients.
	tls := cmd.TLSConfiguration()
	if cmd.tlsConfig, err = server.GetTLSConfig(&tls, cmd.Logger()); err != nil {
		return fmt.Errorf("parsing tls config: %w", err)
	}

	// Create a client to the server.
	client, err := commandClient(cmd)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	cmd.client = client

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

	// Ensure output directory doesn't exist; then create output directory.
	if _, err := os.Stat(cmd.OutputDir); !os.IsNotExist(err) {
		return fmt.Errorf("output directory already exists")
	} else if err := os.MkdirAll(cmd.OutputDir, 0777); err != nil {
		return err
	}

	// Backup schema.
	if err := cmd.backupSchema(ctx, schema); err != nil {
		return fmt.Errorf("cannot back up schema: %w", err)
	} else if err := cmd.backupIDAllocData(ctx); err != nil {
		return fmt.Errorf("cannot back up id alloc data: %w", err)
	}

	// Backup data for each index.
	for _, ii := range schema.Indexes {
		if err := cmd.backupIndexData(ctx, ii); err != nil {
			return err
		}
	}
	// Backup translation data. This has to happen separately, because
	// otherwise a field which uses foreign key translation can reasonably
	// contain values which got created for the foreign index after we
	// backed up that index.
	for _, ii := range schema.Indexes {
		if err := cmd.backupIndexTranslation(ctx, ii); err != nil {
			return err
		}
	}

	// Wait for the OS to persist all directories.
	err = cmd.syncDirectories(ctx)
	if err != nil {
		return fmt.Errorf("syncing directories: %w", err)
	}

	return nil
}

// backupSchema writes the schema to the archive.
func (cmd *BackupCommand) backupSchema(ctx context.Context, schema *pilosa.Schema) error {
	logger := cmd.Logger()
	logger.Printf("backing up schema")

	buf, err := json.MarshalIndent(schema, "", "\t")
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}

	if err := ioutil.WriteFile(filepath.Join(cmd.OutputDir, "schema"), buf, 0666); err != nil {
		return fmt.Errorf("writing schema: %w", err)
	}

	return nil
}

func (cmd *BackupCommand) backupIDAllocData(ctx context.Context) error {
	logger := cmd.Logger()
	logger.Printf("backing up id alloc data")

	rc, err := cmd.client.IDAllocDataReader(ctx)
	if err != nil {
		return fmt.Errorf("fetching id alloc data reader: %w", err)
	}
	defer rc.Close()

	f, err := os.Create(filepath.Join(cmd.OutputDir, "idalloc"))
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return err
	} else if err := cmd.syncFile(f); err != nil {
		return err
	}
	return f.Close()
}

// backupIndexTranslation backs up both field and index-wide key translation for
// the given index. it has to run after the index's data has been backed up,
// but also after the data for any index which might have a foreign-key
// relation to this index has been backed up.
func (cmd *BackupCommand) backupIndexTranslation(ctx context.Context, ii *pilosa.IndexInfo) error {
	logger := cmd.Logger()
	logger.Printf("backing up index translation: %q", ii.Name)
	if err := cmd.backupIndexTranslateData(ctx, ii.Name); err != nil {
		return err
	}

	// Back up field translation data.
	for _, fi := range ii.Fields {
		if err := cmd.backupFieldTranslateData(ctx, ii.Name, fi.Name); err != nil {
			return fmt.Errorf("cannot backup field translation data for field %q on index %q: %w", fi.Name, ii.Name, err)
		}
	}

	return nil
}

// backupIndexData backs up all shard data for a given index.
func (cmd *BackupCommand) backupIndexData(ctx context.Context, ii *pilosa.IndexInfo) error {
	logger := cmd.Logger()
	logger.Printf("backing up index data: %q", ii.Name)
	shards, err := cmd.client.AvailableShards(ctx, ii.Name)
	if err != nil {
		return fmt.Errorf("cannot find available shards for index %q: %w", ii.Name, err)
	}

	// Back up all bitmap data for the index.
	ch := make(chan uint64, len(shards))
	for _, shard := range shards {
		ch <- shard
	}
	close(ch)

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < cmd.Concurrency; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case shard, ok := <-ch:
					if !ok {
						return nil
					} else if err := cmd.backupShard(ctx, ii.Name, shard); err != nil {
						return fmt.Errorf("cannot backup shard %d on index %q: %w", shard, ii.Name, err)
					}
				}
			}
		})
	}
	return g.Wait()
}

// backupShard backs up a single shard from a single index.
func (cmd *BackupCommand) backupShard(ctx context.Context, indexName string, shard uint64) (err error) {
	nodes, err := cmd.client.FragmentNodes(ctx, indexName, shard)
	if err != nil {
		return fmt.Errorf("cannot determine fragment nodes: %w", err)
	} else if len(nodes) == 0 {
		return fmt.Errorf("no nodes available")
	}

	for _, node := range nodes {
		if e := cmd.backupShardNode(ctx, indexName, shard, node); e == nil {
			return nil // backup ok, exit
		} else if err == nil {
			err = e // save first error, try next node
		}
	}
	return err
}

// backupShardNode backs up a single shard from a single index on a specific node.
func (cmd *BackupCommand) backupShardNode(ctx context.Context, indexName string, shard uint64, node *topology.Node) error {
	logger := cmd.Logger()
	logger.Printf("backing up shard: index=%q id=%d", indexName, shard)

	client := http.NewInternalClientFromURI(&node.URI, http.GetHTTPClient(cmd.tlsConfig))
	rc, err := client.ShardReader(ctx, indexName, shard)
	if err != nil {
		return fmt.Errorf("fetching shard reader: %w", err)
	}
	defer rc.Close()

	filename := filepath.Join(cmd.OutputDir, "indexes", indexName, "shards", fmt.Sprintf("%04d", shard))
	if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil {
		return err
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return err
	} else if err := cmd.syncFile(f); err != nil {
		return err
	}
	return f.Close()
}

func (cmd *BackupCommand) backupIndexTranslateData(ctx context.Context, name string) error {
	partitionN := topology.DefaultPartitionN

	ch := make(chan int, partitionN)
	for partitionID := 0; partitionID < partitionN; partitionID++ {
		ch <- partitionID
	}
	close(ch)

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < cmd.Concurrency; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case partitionID, ok := <-ch:
					if !ok {
						return nil
					} else if err := cmd.backupIndexPartitionTranslateData(ctx, name, partitionID); err != nil {
						return fmt.Errorf("cannot backup index translation data for partition %d on %q: %w", partitionID, name, err)
					}
				}
			}
		})
	}
	return g.Wait()
}

func (cmd *BackupCommand) backupIndexPartitionTranslateData(ctx context.Context, name string, partitionID int) error {
	logger := cmd.Logger()
	logger.Printf("backing up index translation data: %s/%d", name, partitionID)

	rc, err := cmd.client.IndexTranslateDataReader(ctx, name, partitionID)
	if err != nil {
		return fmt.Errorf("fetching translate data reader: %w", err)
	}
	defer rc.Close()

	filename := filepath.Join(cmd.OutputDir, "indexes", name, "translate", fmt.Sprintf("%04d", partitionID))
	if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil {
		return err
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return err
	} else if err := cmd.syncFile(f); err != nil {
		return err
	}
	return f.Close()
}

func (cmd *BackupCommand) backupFieldTranslateData(ctx context.Context, indexName, fieldName string) error {
	logger := cmd.Logger()
	logger.Printf("backing up field translation data: %s/%s", indexName, fieldName)

	rc, err := cmd.client.FieldTranslateDataReader(ctx, indexName, fieldName)
	if err == pilosa.ErrTranslateStoreNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("fetching translate data reader: %w", err)
	}
	defer rc.Close()

	filename := filepath.Join(cmd.OutputDir, "indexes", indexName, "fields", fieldName, "translate")
	if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil {
		return err
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return err
	} else if err := cmd.syncFile(f); err != nil {
		return err
	}
	return f.Close()
}

func (cmd *BackupCommand) syncFile(f *os.File) error {
	if cmd.NoSync {
		return nil
	}
	return f.Sync()
}

func (cmd *BackupCommand) TLSHost() string { return cmd.Host }

func (cmd *BackupCommand) TLSConfiguration() server.TLSConfig { return cmd.TLS }

// syncDirectories fsyncs all directories required for the backup to be persisted to the filesystem.
func (cmd *BackupCommand) syncDirectories(ctx context.Context) error {
	if cmd.NoSync {
		return nil
	}

	syncChan := make(chan string, cmd.Concurrency)
	syncChan <- filepath.Dir(cmd.OutputDir)
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < cmd.Concurrency; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case path, ok := <-syncChan:
					if !ok {
						return nil
					} else if err := cmd.syncDir(path); err != nil {
						return fmt.Errorf("cannot sync directory %q: %w", path, err)
					}
				}
			}
		})
	}

	err := filepath.Walk(cmd.OutputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case syncChan <- path:
			}
		}

		return nil
	})
	close(syncChan)
	if err != nil {
		return fmt.Errorf("walking output directory tree: %w", err)
	}

	return g.Wait()
}

func (cmd *BackupCommand) syncDir(path string) error {
	logger := cmd.Logger()
	logger.Printf("syncing directory: %s", path)

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening directory for sync: %w", err)
	}
	defer f.Close()

	err = f.Sync()
	if err != nil {
		return fmt.Errorf("syncing directory: %w", err)
	}

	return f.Close()
}
