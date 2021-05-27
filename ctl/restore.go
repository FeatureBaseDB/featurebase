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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	gohttp "net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/topology"
)

// RestoreCommand represents a command for restoring a backup to
type RestoreCommand struct {
	tlsConfig *tls.Config
	Host      string

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
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run executes the restore.
func (cmd *RestoreCommand) Run(ctx context.Context) (err error) {
	logger := cmd.Logger()

	// Validate arguments.
	if cmd.Path == "" {
		return fmt.Errorf("-s flag required")
	}
	useStdin := cmd.Path == "-"

	var f *os.File
	// read from Stdin if path specified as -
	if useStdin {
		f = os.Stdin
	} else {
		f, err = os.Open(cmd.Path)
		if err != nil {
			return (err)
		}
		defer f.Close()
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
			nodes, err := cmd.client.FragmentNodes(ctx, indexName, shard)
			if err != nil {
				return fmt.Errorf("cannot determine fragment nodes: %w", err)
			} else if len(nodes) == 0 {
				return fmt.Errorf("no nodes available")
			}
			wg := new(sync.WaitGroup)
			wg.Add(len(nodes))
			errors := make(chan error)
			finished := make(chan bool)

			shardBytes, err := ioutil.ReadAll(tarReader) // this feels wrong but works for now

			for _, node := range nodes {
				go func() {
					defer wg.Done()
					client := &gohttp.Client{}
					rd := bytes.NewReader(shardBytes)
					logger.Printf("shard %v %v", shard, indexName)
					url := node.URI.Path(fmt.Sprintf("/internal/restore/%v/%v", indexName, shard))
					_, err = client.Post(url, "application/octet-stream", rd)
					if err != nil {
						errors <- err
					}
				}()
			}
			go func() {
				wg.Wait()
				close(finished)
			}()
			var problems error
			select {
			case <-finished:
				break
			case err := <-errors:
				logger.Printf("error sending %v", err)
				problems = err
			}
			if problems != nil {
				return problems
			}

		case "translate":
			partitionID, err := strconv.Atoi(record[3])
			logger.Printf("column keys %v (%v)", indexName, partitionID)
			if err != nil {
				return err
			}
			partitionNodes, err := cmd.client.PartitionNodes(ctx, partitionID)
			wg := new(sync.WaitGroup)
			wg.Add(len(partitionNodes))
			errors := make(chan error)
			finished := make(chan bool)

			shardBytes, err := ioutil.ReadAll(tarReader) // this feels wrong but works for now
			if err != nil {
				return err
			}
			for _, n := range partitionNodes {
				node := n
				go func() {
					defer wg.Done()
					rd := bytes.NewReader(shardBytes)
					err = cmd.client.ImportIndexKeys(ctx, &node.URI, indexName, partitionID, false, rd)
					if err != nil {
						errors <- err
					}
				}()
			}
			go func() {
				wg.Wait()
				close(finished)
			}()
			var problems error
			select {
			case <-finished:
				break
			case err := <-errors:
				logger.Printf("error sending %v", err)
				problems = err
			}
			if problems != nil {
				return problems
			}

		case "attributes":
			//skip
		case "fields":
			fieldName := record[3]
			switch action := record[4]; action {
			case "translate":
				logger.Printf("field keys %v %v", indexName, fieldName)
				/*
					err := cmd.client.ImportFieldKeys(ctx, &node.URI, indexName, fieldName, false, tarReader)
					if err != nil {
						return err
					}
				*/
				//needs to go to all nodes
				wg := new(sync.WaitGroup)
				wg.Add(len(nodes))
				errors := make(chan error)
				finished := make(chan bool)

				shardBytes, err := ioutil.ReadAll(tarReader) // this feels wrong but works for now
				if err != nil {
					return err
				}
				for _, n := range nodes {
					node := n
					go func() {
						defer wg.Done()
						rd := bytes.NewReader(shardBytes)
						err := cmd.client.ImportFieldKeys(ctx, &node.URI, indexName, fieldName, false, rd)
						if err != nil {
							errors <- err
						}
					}()
				}
				go func() {
					wg.Wait()
					close(finished)
				}()
				var problems error
				select {
				case <-finished:
					break
				case err := <-errors:
					logger.Printf("error sending %v", err)
					problems = err
				}
				if problems != nil {
					return problems
				}
			case "attributes":
			//skip
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
func (cmd *RestoreCommand) TLSHost() string { return cmd.Host }

func (cmd *RestoreCommand) TLSConfiguration() server.TLSConfig { return cmd.TLS }
