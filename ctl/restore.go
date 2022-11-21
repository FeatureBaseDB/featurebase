// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/authn"
	"github.com/featurebasedb/featurebase/v3/disco"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/server"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// TODO(rdp): add refresh token to this as well

// RestoreCommand represents a command for restoring a backup to
type RestoreCommand struct {
	tlsConfig *tls.Config

	Host string

	Concurrency int

	// Filepath to the backup file.
	Path string

	// Amount of time after first failed request to continue retrying.
	RetryPeriod time.Duration `json:"retry-period"`

	// Host:port on which to listen for pprof.
	Pprof string `json:"pprof"`

	// Reusable client.
	client *pilosa.InternalClient

	// Standard input/output
	*pilosa.CmdIO

	TLS server.TLSConfig

	AuthToken string
}

// NewRestoreCommand returns a new instance of RestoreCommand.
func NewRestoreCommand(stdin io.Reader, stdout, stderr io.Writer) *RestoreCommand {
	return &RestoreCommand{
		CmdIO:       pilosa.NewCmdIO(stdin, stdout, stderr),
		RetryPeriod: time.Second * 30,
		Concurrency: 1,
		Pprof:       "localhost:0",
	}
}

// Run executes the restore.
func (cmd *RestoreCommand) Run(ctx context.Context) (err error) {
	logger := cmd.Logger()
	close, err := startProfilingServer(cmd.Pprof, logger)
	if err != nil {
		return errors.Wrap(err, "starting profiling server")
	}
	defer close()

	// Validate arguments.
	if cmd.Path == "" {
		return fmt.Errorf("%w: -s flag required", UsageError)
	} else if cmd.Concurrency <= 0 {
		return fmt.Errorf("%w: concurrency must be at least one", UsageError)
	}

	// Parse TLS configuration for node-specific clients.
	tls := cmd.TLSConfiguration()
	if cmd.tlsConfig, err = server.GetTLSConfig(&tls, logger); err != nil {
		return fmt.Errorf("parsing tls config: %w", err)
	}
	// Create a client to the server.
	client, err := commandClient(cmd, pilosa.WithClientRetryPeriod(cmd.RetryPeriod))
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	cmd.client = client

	if cmd.AuthToken != "" {
		ctx = authn.WithAccessToken(ctx, "Bearer "+cmd.AuthToken)
	}

	nodes, err := cmd.client.Nodes(ctx)
	if err != nil {
		return err
	}

	var primary *disco.Node
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
	} else if err := cmd.restoreDataframes(ctx); err != nil {
		return fmt.Errorf("cannot restore dataframes: %w", err)
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

func (cmd *RestoreCommand) restoreSchema(ctx context.Context, primary *disco.Node) error {
	f, err := os.Open(filepath.Join(cmd.Path, "schema"))
	if err != nil {
		return err
	}
	defer f.Close()

	existingSchema, err := cmd.client.Schema(ctx)
	if len(existingSchema) == 0 {
		cmd.Logger().Printf("Load Schema")
		url := primary.URI.Path("/schema")
		req, err := retryablehttp.NewRequest("POST", url, f)
		if err != nil {
			return err
		}
		req = req.WithContext(ctx)
		req.Header.Add("Accept", "application/json")

		token, ok := authn.GetAccessToken(ctx)
		if ok && token != "" {
			req.Header.Set("Authorization", token)
		}

		client := cmd.newClient()
		_, err = client.Do(req)
		if err != nil {
			return err
		}

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
		logger := cmd.Logger()
		// NOTE SHOULD ONLY BE ONE
		for _, index := range schema.Indexes {
			if exists(index.Name) {
				return fmt.Errorf("index Exists %v", index.Name)
			}
			logger.Printf("Create INDEX %v", index.Name)
			err = cmd.client.CreateIndex(ctx, index.Name, index.Options)
			if err != nil {
				return err
			}
			for _, field := range index.Fields {
				logger.Printf("Create Field %v", field.Name)
				err = cmd.client.CreateFieldWithOptions(ctx, index.Name, field.Name, field.Options)
				if err != nil {
					return err
				}
			}
		}
	}
	return err
}

func retryWith400(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if resp != nil && resp.StatusCode >= 400 { // we have some dumb status codes
		return true, nil
	}
	return retryablehttp.DefaultRetryPolicy(ctx, resp, err)
}

// This logic is taken from featurebase/http/client.go If this logic
// is not the same as what's there, that could be a problem. Ideally
// all network calls from restore would go through the client and this
// would not longer be needed.
func (cmd *RestoreCommand) newClient() *retryablehttp.Client {
	min := time.Millisecond * 100

	// do some math to figure out how many attempts we need to get our
	// total sleep time close to the period
	attempts := math.Log2(float64(cmd.RetryPeriod)) - math.Log2(float64(min))
	attempts += 0.3 // mmmm, fudge
	if attempts < 1 {
		attempts = 1
	}
	client := retryablehttp.NewClient()
	client.RetryWaitMin = min
	client.RetryMax = int(attempts)
	client.CheckRetry = retryWith400
	client.Logger = logger.NopLogger

	return client
}

func (cmd *RestoreCommand) restoreIDAlloc(ctx context.Context, primary *disco.Node) error {
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

	err = cmd.client.IDAllocDataWriter(ctx, f, primary)

	return err
}

func (cmd *RestoreCommand) restoreDataframes(ctx context.Context) error {
	filenames, err := filepath.Glob(filepath.Join(cmd.Path, "indexes", "*", "dataframe", "*"))
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
					} else if err := cmd.restoredDataframeShard(ctx, filename); err != nil {
						return err
					}
				}
			}
		})
	}
	return g.Wait()
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
		req, err := retryablehttp.NewRequest("POST", url, f)
		if err != nil {
			return err
		}
		req = req.WithContext(ctx)
		req.Header.Set("Content-Type", "application/octet-stream")

		token, ok := authn.GetAccessToken(ctx)
		if ok && token != "" {
			req.Header.Set("Authorization", token)
		}

		client := cmd.newClient()
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
			readerFunc := func() (io.Reader, error) {
				return os.Open(filename) // gets used as an HTTP request body and closed by http library
			}

			return cmd.client.ImportIndexKeys(ctx, &node.URI, indexName, partitionID, false, readerFunc)
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *RestoreCommand) restoreFieldTranslation(ctx context.Context, nodes []*disco.Node) error {
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

func (cmd *RestoreCommand) restoreFieldTranslationFile(ctx context.Context, nodes []*disco.Node, filename string) error {
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
			readerFunc := func() (io.Reader, error) {
				return os.Open(filename)
			}

			return cmd.client.ImportFieldKeys(ctx, &node.URI, indexName, fieldName, false, readerFunc)
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *RestoreCommand) TLSHost() string { return cmd.Host }

func (cmd *RestoreCommand) TLSConfiguration() server.TLSConfig { return cmd.TLS }

func (cmd *RestoreCommand) restoredDataframeShard(ctx context.Context, filename string) error {
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
		logger.Printf("dataframe shard %v %v", shard, indexName)

		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer f.Close()

		url := node.URI.Path(fmt.Sprintf("/internal/dataframe/restore/%v/%v", indexName, shard))
		req, err := retryablehttp.NewRequest("POST", url, f)
		if err != nil {
			return err
		}
		req = req.WithContext(ctx)
		req.Header.Set("Content-Type", "application/octet-stream")

		token, ok := authn.GetAccessToken(ctx)
		if ok && token != "" {
			req.Header.Set("Authorization", token)
		}

		client := cmd.newClient()
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
