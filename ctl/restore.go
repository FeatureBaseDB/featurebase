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
	/*
		h := &gohttp.Client{}
		host := "SOMETHING"
		c, err := http.NewInternalClient(host, h)
		if err != nil {
			panic(err)
		}
	*/

	return &RestoreCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
		//		client: c,
	}
}

/* helper to allow for both gz and just plan tar
	f, err := os.Open(cmd.Path)
	if err != nil {
		return (err)
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
return
}
*/
func readSchema(path string) string {
	return "{}"
}

// Run executes the restore.
func (cmd *RestoreCommand) Run(ctx context.Context) error {
	// Create a client to the server.
	client, err := commandClient(cmd)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	cmd.client = client

	f, err := os.Open(cmd.Path)
	if err != nil {
		return (err)
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
	//maybe begin transaction?
	/*
		schemaJson := readSchema(cmd.Path)
		//Push the schema from the archive into the cluster belonging to the host.
		client := &gohttp.Client{}
		url := "FIXME"
		_, err = client.Post(url+"/schema", "application/json", bytes.NewBufferString(schemaJson))
		if err != nil {
			return err
		}
	*/
	//TODO (twg) load schema
	//TODO (twg) load rbf shard
	//TODO (twg) load row keys
	//TODO (twg) load column keys
	//TODO (twg) load row attributes keys
	//TODO (twg) load col attributes keys
	//TODO (twg) load idalloc
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
				vprint.VV("SCHEMA %v", url)
				//schemaBytes, err := ioutil.ReadAll(tarReader)
				_, err = c.Post(url, "application/json", tarReader)
				if err != nil {
					return err
				}

			case "idalloc":
				vprint.VV("Load ids")
			default:
				panic("UNKNOWN " + record[0])

			}
			continue
		}
		indexName := record[1]
		switch record[2] {
		case "shards":
			shard := record[3]
			vprint.VV("shard %v %v", shard, indexName)
		case "translate":
			vprint.VV("column keys %v", indexName)
		case "attributes":
			vprint.VV("column attributes %v", indexName)
		case "fields":
			fieldName := record[3]
			switch action := record[4]; action {
			case "translate":
				vprint.VV("field keys %v %v", indexName, fieldName)
			case "attributes":
				vprint.VV("field attributes %v %v", indexName, fieldName)
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
