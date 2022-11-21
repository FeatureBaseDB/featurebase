// Copyright 2022 Molecula Corp. All rights reserved.
package ctl

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	gohttp "net/http"
	"os"
	"strconv"
	"strings"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/authn"
	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/server"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// RestoreTarCommand represents a command for restoring a backup to
type RestoreTarCommand struct {
	tlsConfig *tls.Config

	Host string

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

// NewRestoreTarCommand returns a new instance of RestoreTarCommand.
func NewRestoreTarCommand(stdin io.Reader, stdout, stderr io.Writer) *RestoreTarCommand {
	return &RestoreTarCommand{
		CmdIO:       pilosa.NewCmdIO(stdin, stdout, stderr),
		RetryPeriod: time.Second * 30,
		Pprof:       "localhost:0",
	}
}

// Run executes the restore.
func (cmd *RestoreTarCommand) Run(ctx context.Context) (err error) {
	logger := cmd.Logger()
	close, err := startProfilingServer(cmd.Pprof, logger)
	if err != nil {
		return errors.Wrap(err, "starting profiling server")
	}
	defer close()

	// Validate arguments.
	if cmd.Path == "" {
		return fmt.Errorf("%w: -s flag required", UsageError)
	}
	useStdin := cmd.Path == "-"

	var f io.Reader
	// read from Stdin if path specified as -
	if useStdin {
		f = cmd.Stdin
	} else {
		file, err := os.Open(cmd.Path)
		if err != nil {
			return err
		}
		defer file.Close()
		f = file
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

	var tarReader *tar.Reader
	if strings.HasSuffix(cmd.Path, "gz") {
		gzf, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		tarReader = tar.NewReader(gzf)
	} else {
		tarReader = tar.NewReader(f)
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
	c := &gohttp.Client{}
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		record := strings.Split(header.Name, "/")
		if len(record) == 1 {
			switch record[0] {
			case "schema":
				logger.Printf("Load Schema")
				url := primary.URI.Path("/schema")
				_, err = c.Post(url, "application/json", tarReader)
				if err != nil {
					return err
				}
			case "idalloc":
				logger.Printf("Load ids")
				url := primary.URI.Path("/internal/idalloc/restore")
				_, err = c.Post(url, "application/octet-stream", tarReader)
				if err != nil {
					return err
				}
			default:
				return err

			}
			continue
		}
		indexName := record[1]
		switch record[2] {
		case "shards":
			shard, err := strconv.ParseUint(record[3], 10, 64)
			if err != nil {
				return err
			}
			fragmentNodes, err := cmd.client.FragmentNodes(ctx, indexName, shard)
			if err != nil {
				return fmt.Errorf("cannot determine fragmentNodes: %w", err)
			} else if len(fragmentNodes) == 0 {
				return fmt.Errorf("no fragmentNodes available")
			}

			shardBytes, err := io.ReadAll(tarReader) // this feels wrong but works for now
			if err != nil {
				return err
			}
			g, _ := errgroup.WithContext(ctx)
			for _, node := range fragmentNodes {
				node := node
				g.Go(func() error {
					client := &gohttp.Client{}
					rd := bytes.NewReader(shardBytes)
					logger.Printf("shard %v %v", shard, indexName)
					url := node.URI.Path(fmt.Sprintf("/internal/restore/%v/%v", indexName, shard))
					_, err = client.Post(url, "application/octet-stream", rd)
					return err
				})
			}
			if err := g.Wait(); err != nil {
				return err
			}
		case "dataframe":
			shard, err := strconv.ParseUint(record[3], 10, 64)
			if err != nil {
				return err
			}
			fragmentNodes, err := cmd.client.FragmentNodes(ctx, indexName, shard)
			if err != nil {
				return fmt.Errorf("cannot determine fragmentNodes: %w", err)
			} else if len(fragmentNodes) == 0 {
				return fmt.Errorf("no fragmentNodes available")
			}

			shardBytes, err := io.ReadAll(tarReader) // this feels wrong but works for now
			if err != nil {
				return err
			}
			g, _ := errgroup.WithContext(ctx)
			for _, node := range fragmentNodes {
				node := node
				g.Go(func() error {
					client := &gohttp.Client{}
					rd := bytes.NewReader(shardBytes)
					logger.Printf("dataframe shard %v %v", shard, indexName)
					url := node.URI.Path(fmt.Sprintf("/internal/dataframe/restore/%v/%v", indexName, shard))
					_, err = client.Post(url, "application/octet-stream", rd)
					return err
				})
			}
			if err := g.Wait(); err != nil {
				return err
			}
		case "translate":
			partitionID, err := strconv.Atoi(record[3])
			logger.Printf("column keys %v (%v)", indexName, partitionID)
			if err != nil {
				return err
			}
			partitionNodes, err := cmd.client.PartitionNodes(ctx, partitionID)
			if err != nil {
				return err
			}
			shardBytes, err := io.ReadAll(tarReader) // this feels wrong but works for now
			if err != nil {
				return err
			}
			g, _ := errgroup.WithContext(ctx)
			for _, node := range partitionNodes {
				node := node
				g.Go(func() error {
					// rd := bytes.NewReader(shardBytes)
					rd := func() (io.Reader, error) {
						return bytes.NewReader(shardBytes), nil
					}

					return cmd.client.ImportIndexKeys(ctx, &node.URI, indexName, partitionID, false, rd)
				})
			}
			if err := g.Wait(); err != nil {
				return err
			}

		case "attributes":
			// skip
		case "fields":
			fieldName := record[3]
			switch action := record[4]; action {
			case "translate":
				logger.Printf("field keys %v %v", indexName, fieldName)
				// needs to go to all nodes
				shardBytes, err := io.ReadAll(tarReader) // this feels wrong but works for now
				if err != nil {
					return err
				}
				g, _ := errgroup.WithContext(ctx)
				for _, node := range nodes {
					node := node
					g.Go(func() error {
						// rd := bytes.NewReader(shardBytes)
						rd := func() (io.Reader, error) {
							return bytes.NewReader(shardBytes), nil
						}

						return cmd.client.ImportFieldKeys(ctx, &node.URI, indexName, fieldName, false, rd)
					})
				}
				if err := g.Wait(); err != nil {
					return err
				}
			case "attributes":
			// skip
			default:
				return fmt.Errorf("unknown restore action: %v", action)
			}

		}

	}
	/*	Fetch the cluster nodes from the target host.
		For each index:
		Upload the RBF snapshot for each shard to the nodes that own the shard.
		Upload the index & field translation BoltDB snapshots to each node.
		If possible, trigger the node to reload itself. Otherwise a restart would be required.
	*/

	return nil
}
func (cmd *RestoreTarCommand) TLSHost() string { return cmd.Host }

func (cmd *RestoreTarCommand) TLSConfiguration() server.TLSConfig { return cmd.TLS }
