// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ctl

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/topology"
)

// BackupCommand represents a command for backing up a Pilosa node.
type BackupCommand struct { // nolint: maligned
	// Destination host and port.
	Host string `json:"host"`

	// Path to write the backup to.
	OutputPath string

	// Reusable client.
	client pilosa.InternalClient

	// Standard input/output
	*pilosa.CmdIO

	TLS server.TLSConfig
}

// NewBackupCommand returns a new instance of BackupCommand.
func NewBackupCommand(stdin io.Reader, stdout, stderr io.Writer) *BackupCommand {
	return &BackupCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// TempPath returns the path to the temporary file to write the archive to.
func (cmd *BackupCommand) TempPath() string {
	dir, base := filepath.Split(cmd.OutputPath)
	return filepath.Join(dir, "."+base)
}

// Run executes the main program execution.
func (cmd *BackupCommand) Run(ctx context.Context) error {
	logger := cmd.Logger()

	// Validate arguments.
	if cmd.OutputPath == "" {
		return fmt.Errorf("-o flag required")
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
	schema := &pilosa.Schema{Indexes: indexes}

	// Create output file in temporary location.
	w, err := os.Create(cmd.OutputPath + ".tmp")
	if err != nil {
		return err
	}
	defer w.Close()

	// Open a tar/gzip writer to the temporary file.
	gw := gzip.NewWriter(w)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	// Backup schema.
	if err := cmd.backupSchema(ctx, tw, schema); err != nil {
		return fmt.Errorf("cannot back up schema: %w", err)
	} else if err := cmd.backupIDAllocData(ctx, tw); err != nil {
		return fmt.Errorf("cannot back up id alloc data: %w", err)
	}

	// Backup data for each index.
	for _, ii := range schema.Indexes {
		if err := cmd.backupIndex(ctx, tw, ii); err != nil {
			return err
		}
	}

	// Move data file to final location.
	logger.Printf("writing backup: %s", cmd.OutputPath)
	if err := os.Rename(cmd.OutputPath+".tmp", cmd.OutputPath); err != nil {
		return err
	}

	return nil
}

// backupSchema writes the schema to the archive.
func (cmd *BackupCommand) backupSchema(ctx context.Context, tw *tar.Writer, schema *pilosa.Schema) error {
	logger := cmd.Logger()
	logger.Printf("backing up schema")

	buf, err := json.MarshalIndent(schema, "", "\t")
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}

	// Build header & copy data to archive.
	if err = tw.WriteHeader(&tar.Header{
		Name:    "schema",
		Mode:    0666,
		Size:    int64(len(buf)),
		ModTime: time.Now(),
	}); err != nil {
		return err
	} else if _, err := tw.Write(buf); err != nil {
		return fmt.Errorf("copying schema to archive: %w", err)
	}

	return nil
}

func (cmd *BackupCommand) backupIDAllocData(ctx context.Context, tw *tar.Writer) error {
	logger := cmd.Logger()
	logger.Printf("backing up id alloc data")

	rc, err := cmd.client.IDAllocDataReader(ctx)
	if err != nil {
		return fmt.Errorf("fetching id alloc data reader: %w", err)
	}
	defer rc.Close()

	// Read to buffer to determine size.
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return fmt.Errorf("copying id alloc data to memory: %w", err)
	}

	// Build header & copy data to archive.
	if err = tw.WriteHeader(&tar.Header{
		Name:    "idalloc",
		Mode:    0666,
		Size:    int64(buf.Len()),
		ModTime: time.Now(),
	}); err != nil {
		return err
	} else if _, err := io.Copy(tw, &buf); err != nil {
		return fmt.Errorf("copying id alloc data to archive: %w", err)
	}

	return nil
}

// backupIndex backs up all shards for a given index.
func (cmd *BackupCommand) backupIndex(ctx context.Context, tw *tar.Writer, ii *pilosa.IndexInfo) error {
	logger := cmd.Logger()
	logger.Printf("backing up index: %q", ii.Name)

	shards, err := cmd.client.AvailableShards(ctx, ii.Name)
	if err != nil {
		return fmt.Errorf("cannot find available shards for index %q: %w", ii.Name, err)
	}

	// Back up all bitmap data for the index.
	for _, shard := range shards {
		if err := cmd.backupShard(ctx, tw, ii.Name, shard); err != nil {
			return fmt.Errorf("cannot backup shard %d on index %q: %w", shard, ii.Name, err)
		}
	}

	// Back up translation data after bitmap data so we ensure we can translate all data.
	if err := cmd.backupIndexTranslateData(ctx, tw, ii.Name); err != nil {
		return err
	}
	if err := cmd.backupIndexAttrData(ctx, tw, ii.Name); err != nil {
		return err
	}

	// Back up field translation & attribute data.
	for _, fi := range ii.Fields {
		if err := cmd.backupFieldTranslateData(ctx, tw, ii.Name, fi.Name); err != nil {
			return fmt.Errorf("cannot backup field translation data for field %q on index %q: %w", fi.Name, ii.Name, err)
		}
		if err := cmd.backupFieldAttrData(ctx, tw, ii.Name, fi.Name); err != nil {
			return fmt.Errorf("cannot backup field attr data for field %q on index %q: %w", fi.Name, ii.Name, err)
		}
	}

	return nil
}

// backupShard backs up a single shard from a single index.
func (cmd *BackupCommand) backupShard(ctx context.Context, tw *tar.Writer, indexName string, shard uint64) error {
	logger := cmd.Logger()
	logger.Printf("backing up shard: index=%q id=%d", indexName, shard)

	filename := path.Join("indexes", indexName, "shards", fmt.Sprintf("%04d", shard))

	rc, err := cmd.client.ShardReader(ctx, indexName, shard)
	if err != nil {
		return fmt.Errorf("fetching shard reader: %w", err)
	}
	defer rc.Close()

	// Read to buffer to determine size.
	// TODO: Provide size via the reader itself.
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return fmt.Errorf("copying shard data to memory: %w", err)
	}

	// Build header & copy data to archive.
	if err = tw.WriteHeader(&tar.Header{
		Name:    filename,
		Mode:    0666,
		Size:    int64(buf.Len()),
		ModTime: time.Now(),
	}); err != nil {
		return err
	} else if _, err := io.Copy(tw, &buf); err != nil {
		return fmt.Errorf("copying shard data to archive: %w", err)
	}

	return nil
}

func (cmd *BackupCommand) backupIndexTranslateData(ctx context.Context, tw *tar.Writer, name string) error {
	// TODO: Fetch holder partition count.
	partitionN := topology.DefaultPartitionN
	for partitionID := 0; partitionID < partitionN; partitionID++ {
		if err := cmd.backupIndexPartitionTranslateData(ctx, tw, name, partitionID); err != nil {
			return fmt.Errorf("cannot backup index translation data for partition %d on %q: %w", partitionID, name, err)
		}
	}
	return nil
}

func (cmd *BackupCommand) backupIndexPartitionTranslateData(ctx context.Context, tw *tar.Writer, name string, partitionID int) error {
	logger := cmd.Logger()
	logger.Printf("backing up index translation data: %s/%d", name, partitionID)

	rc, err := cmd.client.IndexTranslateDataReader(ctx, name, partitionID)
	if err == pilosa.ErrTranslateStoreNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("fetching translate data reader: %w", err)
	}
	defer rc.Close()

	// Read to buffer to determine size.
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return fmt.Errorf("copying translate data to memory: %w", err)
	}

	// Build header & copy data to archive.
	if err = tw.WriteHeader(&tar.Header{
		Name:    path.Join("indexes", name, "translate", fmt.Sprintf("%04d", partitionID)),
		Mode:    0666,
		Size:    int64(buf.Len()),
		ModTime: time.Now(),
	}); err != nil {
		return err
	} else if _, err := io.Copy(tw, &buf); err != nil {
		return fmt.Errorf("copying translate data to archive: %w", err)
	}

	return nil
}

func (cmd *BackupCommand) backupIndexAttrData(ctx context.Context, tw *tar.Writer, name string) error {
	logger := cmd.Logger()
	logger.Printf("backing up index attr data: %s", name)

	rc, err := cmd.client.IndexAttrDataReader(ctx, name)
	if err != nil {
		return fmt.Errorf("fetching index attr data reader: %w", err)
	}
	defer rc.Close()

	// Read to buffer to determine size.
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return fmt.Errorf("copying index attr data to memory: %w", err)
	}

	// Build header & copy data to archive.
	if err = tw.WriteHeader(&tar.Header{
		Name:    path.Join("indexes", name, "attributes"),
		Mode:    0666,
		Size:    int64(buf.Len()),
		ModTime: time.Now(),
	}); err != nil {
		return err
	} else if _, err := io.Copy(tw, &buf); err != nil {
		return fmt.Errorf("copying index attr data to archive: %w", err)
	}
	return nil
}

func (cmd *BackupCommand) backupFieldTranslateData(ctx context.Context, tw *tar.Writer, indexName, fieldName string) error {
	logger := cmd.Logger()
	logger.Printf("backing up field translation data: %s/%s", indexName, fieldName)

	rc, err := cmd.client.FieldTranslateDataReader(ctx, indexName, fieldName)
	if err == pilosa.ErrTranslateStoreNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("fetching translate data reader: %w", err)
	}
	defer rc.Close()

	// Read to buffer to determine size.
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return fmt.Errorf("copying translate data to memory: %w", err)
	}

	// Build header & copy data to archive.
	if err = tw.WriteHeader(&tar.Header{
		Name:    path.Join("indexes", indexName, "fields", fieldName, "translate"),
		Mode:    0666,
		Size:    int64(buf.Len()),
		ModTime: time.Now(),
	}); err != nil {
		return err
	} else if _, err := io.Copy(tw, &buf); err != nil {
		return fmt.Errorf("copying translate data to archive: %w", err)
	}
	return nil
}

func (cmd *BackupCommand) backupFieldAttrData(ctx context.Context, tw *tar.Writer, indexName, fieldName string) error {
	logger := cmd.Logger()
	logger.Printf("backing up field attr data: %s/%s", indexName, fieldName)

	rc, err := cmd.client.FieldAttrDataReader(ctx, indexName, fieldName)
	if err != nil {
		return fmt.Errorf("fetching field attr data reader: %w", err)
	}
	defer rc.Close()

	// Read to buffer to determine size.
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return fmt.Errorf("copying field attr data to memory: %w", err)
	}

	// Build header & copy data to archive.
	if err = tw.WriteHeader(&tar.Header{
		Name:    path.Join("indexes", indexName, "fields", fieldName, "attributes"),
		Mode:    0666,
		Size:    int64(buf.Len()),
		ModTime: time.Now(),
	}); err != nil {
		return err
	} else if _, err := io.Copy(tw, &buf); err != nil {
		return fmt.Errorf("copying field attr data to archive: %w", err)
	}
	return nil
}

func (cmd *BackupCommand) TLSHost() string { return cmd.Host }

func (cmd *BackupCommand) TLSConfiguration() server.TLSConfig { return cmd.TLS }
