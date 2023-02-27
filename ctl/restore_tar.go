// Copyright 2022 Molecula Corp. All rights reserved.
package ctl

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	gohttp "net/http"
	"os"
	"strconv"
	"strings"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/authn"
	"github.com/featurebasedb/featurebase/v3/buffer"
	"github.com/featurebasedb/featurebase/v3/disco"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/server"
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
	logDest logger.Logger

	TLS server.TLSConfig

	AuthToken string

	// TempDir location of scratch files
	TempDir string
}

// Logger returns the command's associated Logger to maintain CommandWithTLSSupport interface compatibility
func (cmd *RestoreTarCommand) Logger() logger.Logger {
	return cmd.logDest
}

// NewRestoreTarCommand returns a new instance of RestoreTarCommand.
func NewRestoreTarCommand(logdest logger.Logger) *RestoreTarCommand {
	return &RestoreTarCommand{
		logDest:     logdest,
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
		f = os.Stdin
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
	// buf := new(bytes.Buffer)
	mb512 := 2 << 29
	buf := buffer.NewFileBuffer(mb512, cmd.TempDir)
	defer buf.Reset()
	for {
		buf.Reset()
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
			_, err = io.Copy(buf, tarReader)
			if err != nil {
				return errors.Wrap(err, "copying")
			}
			g, _ := errgroup.WithContext(ctx)
			for _, node := range fragmentNodes {
				node := node
				rd, err := buf.NewReader()
				if err != nil {
					return err
				}
				g.Go(func() error {
					logger.Printf("shard %v %v", shard, indexName)
					url := node.URI.Path(fmt.Sprintf("/internal/restore/%v/%v", indexName, shard))
					return Post(ctx, url, "application/octet-stream", rd, nil)
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

			_, err = io.Copy(buf, tarReader)
			if err != nil {
				return errors.Wrap(err, "copying")
			}

			g, _ := errgroup.WithContext(ctx)
			for _, node := range fragmentNodes {
				node := node
				rd, err := buf.NewReader()
				if err != nil {
					return err
				}
				g.Go(func() error {
					logger.Printf("dataframe shard %v %v", shard, indexName)
					url := node.URI.Path(fmt.Sprintf("/internal/dataframe/restore/%v/%v", indexName, shard))
					return Post(ctx, url, "application/octet-stream", rd, nil)
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

			_, err = io.Copy(buf, tarReader)
			if err != nil {
				return errors.Wrap(err, "copying")
			}

			g, _ := errgroup.WithContext(ctx)
			for _, node := range partitionNodes {
				node := node
				rd, err := buf.NewReader()
				if err != nil {
					return err
				}
				g.Go(func() error {
					url := node.URI.Path(fmt.Sprintf("/internal/translate/index/%s/%d", indexName, partitionID))
					return Post(ctx, url, "application/octet-stream", rd, nil)
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
				_, err = io.Copy(buf, tarReader)
				if err != nil {
					return errors.Wrap(err, "copying")
				}
				g, _ := errgroup.WithContext(ctx)
				for _, node := range nodes {
					node := node
					rd, err := buf.NewReader()
					if err != nil {
						return err
					}
					g.Go(func() error {
						url := node.URI.Path(fmt.Sprintf("/internal/translate/field/%s/%s", indexName, fieldName))
						return Post(ctx, url, "application/octet-stream", rd, nil)
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

func Post(ctx context.Context, url, contentType string, rd io.Reader, query map[string]string) error {
	client := &gohttp.Client{}
	req, err := http.NewRequest(http.MethodPost, url, rd)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)
	req.Header.Set("Content-Type", contentType)
	pilosa.AddAuthToken(ctx, &req.Header)

	// appending to existing query args

	q := req.URL.Query()
	for k, v := range query {
		q.Add(k, v)
	}

	// assign encoded query string to http request
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Errored when sending request to the server")
		return err
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body) // drain the response
	return err
}
