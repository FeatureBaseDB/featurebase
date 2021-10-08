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
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/server"
	"github.com/molecula/featurebase/v2/topology"
	"github.com/molecula/featurebase/v2/vprint"
	"golang.org/x/sync/errgroup"
)

// RestoreCommand represents a command for restoring a backup to
type RestoreCommand struct {
	tlsConfig *tls.Config
	Host      string

	Concurrency int

	// Filepath to the backup file.
	Path string
	// Reusable client.
	client pilosa.InternalClient

	// Standard input/output
	*pilosa.CmdIO
	TLS server.TLSConfig
}

// NewRestoreCommand returns a new instance of RestoreCommand.
func NewRestoreCommand(stdin io.Reader, stdout, stderr io.Writer) *RestoreCommand {
	return &RestoreCommand{
		CmdIO:       pilosa.NewCmdIO(stdin, stdout, stderr),
		Concurrency: 1,
	}
}

// Run executes the restore.
func (cmd *RestoreCommand) Run(ctx context.Context) (err error) {
	logger := cmd.Logger()

	// Validate arguments.
	if cmd.Path == "" {
		return fmt.Errorf("-s flag required")
	} else if cmd.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be at least one")
	}

	// Parse TLS configuration for node-specific clients.
	tls := cmd.TLSConfiguration()
	if cmd.tlsConfig, err = server.GetTLSConfig(&tls, logger); err != nil {
		return fmt.Errorf("parsing tls config: %w", err)
	}
	// Create a client to the server.
	client, err := commandClient(cmd)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	cmd.client = client

	nodes, err := cmd.client.Nodes(ctx)
	if err != nil {
		return err
	}

	var primary *topology.Node
	for _, node := range nodes {
		if node.IsPrimary {
			primary = node
			break
		}

	}
	if primary == nil {
		return errors.New("no primary")
	}

	if err := cmd.restoreSchema(ctx, primary); err != nil {
		return fmt.Errorf("cannot restore schema: %w", err)
	} else if err := cmd.restoreIDAlloc(ctx, primary); err != nil {
		return fmt.Errorf("cannot restore idalloc: %w", err)
	}
	if err := cmd.restoreShards(ctx); err != nil {
		return fmt.Errorf("cannot restore shards: %w", err)
	} else if err := cmd.restoreIndexTranslation(ctx); err != nil {
		return fmt.Errorf("cannot restore index translation: %w", err)
	} else if err := cmd.restoreFieldTranslation(ctx, nodes); err != nil {
		return fmt.Errorf("cannot restore field translation: %w", err)
	}

	/*	Fetch the cluster nodes from the target host.
		For each index:
		Upload the RBF snapshot for each shard to the nodes that own the shard.
		Upload the index & field translation BoltDB snapshots to each node.
		If possible, trigger the node to reload itself. Otherwise a restart would be required.
	*/

	return nil
}

func (cmd *RestoreCommand) restoreSchema(ctx context.Context, primary *topology.Node) error {
	f, err := os.Open(filepath.Join(cmd.Path, "schema"))
	if err != nil {
		return err
	}
	defer f.Close()

	existingSchema, err := cmd.client.Schema(ctx)
	if len(existingSchema) == 0 {
		cmd.Logger().Printf("Load Schema")
		url := primary.URI.Path("/schema")
		var client http.Client
		_, err = client.Post(url, "application/json", f)
	} else {
		schema := &pilosa.Schema{}
		if err := json.NewDecoder(f).Decode(schema); err != nil {
			if err != nil {
				return err
			}
		}
		exists := func(indexName string) bool {
			for _, i := range existingSchema {
				if i.Name == indexName {
					return true
				}
			}
			return false
		}
		//NOTE SHOULD ONLY BE ONE
		for _, index := range schema.Indexes {
			if exists(index.Name) {
				return errors.New(fmt.Sprintf("Index Exists %v", index.Name))
			}
			vprint.VV("Create INDEX %v", index.Name)
			err = cmd.client.CreateIndex(ctx, index.Name, index.Options)
			if err != nil {
				return err
			}
			for _, field := range index.Fields {
				vprint.VV("Create Field %v", field.Name)
				err = cmd.client.CreateFieldWithOptions(ctx, index.Name, field.Name, field.Options)
				if err != nil {
					return err
				}
			}
		}
	}
	return err
}

func (cmd *RestoreCommand) restoreIDAlloc(ctx context.Context, primary *topology.Node) error {
	logger := cmd.Logger()

	f, err := os.Open(filepath.Join(cmd.Path, "idalloc"))
	if os.IsNotExist(err) {
		logger.Printf("No idalloc, skipping")
		return nil
	} else if err != nil {
		return err
	}
	defer f.Close()

	logger.Printf("Load idalloc")
	url := primary.URI.Path("/internal/idalloc/restore")

	var client http.Client
	_, err = client.Post(url, "application/octet-stream", f)
	return err
}

func (cmd *RestoreCommand) restoreShards(ctx context.Context) error {
	filenames, err := filepath.Glob(filepath.Join(cmd.Path, "indexes", "*", "shards", "*"))
	if err != nil {
		return err
	}

	ch := make(chan string, len(filenames))
	for _, filename := range filenames {
		ch <- filename
	}
	close(ch)

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < cmd.Concurrency; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case filename, ok := <-ch:
					if !ok {
						return nil
					} else if err := cmd.restoreShard(ctx, filename); err != nil {
						return err
					}
				}
			}
		})
	}
	return g.Wait()
}

func (cmd *RestoreCommand) restoreShard(ctx context.Context, filename string) error {
	logger := cmd.Logger()

	rel, err := filepath.Rel(cmd.Path, filename)
	if err != nil {
		return err
	}

	// Parse filename.
	record := strings.Split(rel, string(os.PathSeparator))
	indexName := record[1]
	shard, err := strconv.ParseUint(record[3], 10, 64)
	if err != nil {
		return nil // not a shard file
	}

	nodes, err := cmd.client.FragmentNodes(ctx, indexName, shard)
	if err != nil {
		return fmt.Errorf("cannot determine fragment nodes: %w", err)
	} else if len(nodes) == 0 {
		return fmt.Errorf("no nodes available")
	}

	for _, node := range nodes {
		logger.Printf("shard %v %v", shard, indexName)

		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer f.Close()

		url := node.URI.Path(fmt.Sprintf("/internal/restore/%v/%v", indexName, shard))
		req, err := http.NewRequest("POST", url, f)
		if err != nil {
			return err
		}
		req = req.WithContext(ctx)
		req.Header.Set("Content-Type", "application/octet-stream")

		var client http.Client
		resp, err := client.Do(req)
		if err != nil {
			return err
		} else if err := resp.Body.Close(); err != nil {
			return err
		} else if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
	}
	return nil
}

func (cmd *RestoreCommand) restoreIndexTranslation(ctx context.Context) error {
	filenames, err := filepath.Glob(filepath.Join(cmd.Path, "indexes", "*", "translate", "*"))
	if err != nil {
		return err
	}

	ch := make(chan string, len(filenames))
	for _, filename := range filenames {
		ch <- filename
	}
	close(ch)

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < cmd.Concurrency; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case filename, ok := <-ch:
					if !ok {
						return nil
					} else if err := cmd.restoreIndexTranslationFile(ctx, filename); err != nil {
						return err
					}
				}
			}
		})
	}
	return g.Wait()
}

func (cmd *RestoreCommand) restoreIndexTranslationFile(ctx context.Context, filename string) error {
	logger := cmd.Logger()

	rel, err := filepath.Rel(cmd.Path, filename)
	if err != nil {
		return err
	}

	record := strings.Split(rel, string(os.PathSeparator))
	indexName := record[1]
	partitionID, err := strconv.Atoi(record[3])
	if err != nil {
		return err
	}
	logger.Printf("column keys %v (%v)", indexName, partitionID)

	nodes, err := cmd.client.PartitionNodes(ctx, partitionID)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		if err := func() error {
			f, err := os.Open(filename)
			if err != nil {
				return err
			}
			defer f.Close()

			return cmd.client.ImportIndexKeys(ctx, &node.URI, indexName, partitionID, false, f)
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *RestoreCommand) restoreFieldTranslation(ctx context.Context, nodes []*topology.Node) error {
	filenames, err := filepath.Glob(filepath.Join(cmd.Path, "indexes", "*", "fields", "*", "translate"))
	if err != nil {
		return err
	}

	ch := make(chan string, len(filenames))
	for _, filename := range filenames {
		ch <- filename
	}
	close(ch)

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < cmd.Concurrency; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case filename, ok := <-ch:
					if !ok {
						return nil
					} else if err := cmd.restoreFieldTranslationFile(ctx, nodes, filename); err != nil {
						return err
					}
				}
			}
		})
	}
	return g.Wait()
}

func (cmd *RestoreCommand) restoreFieldTranslationFile(ctx context.Context, nodes []*topology.Node, filename string) error {
	logger := cmd.Logger()

	rel, err := filepath.Rel(cmd.Path, filename)
	if err != nil {
		return err
	}

	record := strings.Split(rel, string(os.PathSeparator))
	indexName, fieldName := record[1], record[3]

	logger.Printf("field keys %v %v", indexName, fieldName)

	for _, node := range nodes {
		if err := func() error {
			f, err := os.Open(filename)
			if err != nil {
				return err
			}
			defer f.Close()

			return cmd.client.ImportFieldKeys(ctx, &node.URI, indexName, fieldName, false, f)
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *RestoreCommand) TLSHost() string { return cmd.Host }

func (cmd *RestoreCommand) TLSConfiguration() server.TLSConfig { return cmd.TLS }
