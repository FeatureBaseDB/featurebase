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
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	gohttp "net/http"
	"os"
	"strconv"
	"strings"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/topology"
	"github.com/pilosa/pilosa/v2/vprint"
)

// RestoreCommand represents a command for restoring a backup to
type RestoreCommand struct {
	// Filepath to the backup file.
	Path string
	Host string
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
func (cmd *RestoreCommand) Run(ctx context.Context) error {
	// Create a client to the server.
	client, err := commandClient(cmd)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	cmd.client = client
	var f *os.File
	// read from Stdin if path specified as -
	if cmd.Path == "-" {
		f = os.Stdin
	} else {
		f, err = os.Open(cmd.Path)
		if err != nil {
			return (err)
		}
	}
	defer f.Close()
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
	c := &gohttp.Client{}
	if primary == nil {
		return errors.New("no primary")
	}
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		record := strings.Split(header.Name, "/")
		if len(record) == 1 {
			switch record[0] {
			case "schema":
				vprint.VV("Load Schema")
				url := primary.URI.Path("/schema")
				_, err = c.Post(url, "application/json", tarReader)
				if err != nil {
					return err
				}
			case "idalloc":
				vprint.VV("Load ids")
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
			shard, err := strconv.Atoi(record[3])
			if err != nil {
				return err
			}
			vprint.VV("shard %v %v", shard, indexName)
			url := primary.URI.Path(fmt.Sprintf("/internal/restore/%v/%v", indexName, shard))
			//TODO (twg) cluster aware client
			_, err = c.Post(url, "application/octet-stream", tarReader)
			if err != nil {
				return err
			}
		case "translate":
			partitionID, err := strconv.Atoi(record[3])
			vprint.VV("column keys %v (%v)", indexName, partitionID)
			if err != nil {
				return err
			}

			err = cmd.client.ImportIndexKeys(ctx, &primary.URI, indexName, partitionID, false, tarReader)
			if err != nil {
				return err
			}
		case "attributes":
			//skip
			//vprint.VV("column attributes %v", indexName)
		case "fields":
			fieldName := record[3]
			switch action := record[4]; action {
			case "translate":
				vprint.VV("field keys %v %v", indexName, fieldName)
				err := cmd.client.ImportFieldKeys(ctx, &primary.URI, indexName, fieldName, false, tarReader)
				if err != nil {
					return err
				}
			case "attributes":
			//	vprint.VV("field attributes %v %v", indexName, fieldName)
			default:
				panic("unknown:" + action)
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
