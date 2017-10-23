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
	"encoding/gob"
	"errors"
	"io"
	"log"
	"os"
	"time"

	"github.com/pilosa/pilosa"
)

type BackupCommand struct {
	// Destination host and port.
	Host string

	// Output file to write to.
	Path string

	// Standard input/output
	*pilosa.CmdIO

	TLS pilosa.TLSConfig
}

func NewBackupCommand(stdin io.Reader, stdout, stderr io.Writer) *BackupCommand {
	return &BackupCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

func (cmd *BackupCommand) Run(ctx context.Context) (err error) {
	// Validate arguments.
	if cmd.Path == "" {
		return errors.New("output file required")
	}

	// Create a client to the server.
	client, err := CommandClient(cmd)
	if err != nil {
		return err
	}

	schema, err := client.Schema(ctx)
	if err != nil {
		return err
	}

	var w = cmd.Stdout
	if cmd.Path != "" {

		filename := cmd.Path + ".pak"
		log.Println("Creating:", filename)
		f, err := os.Create(filename)
		defer f.Close()
		if err != nil {
			return err
		}
		w = f
	}
	gw := gzip.NewWriter(w)
	tw := tar.NewWriter(gw)
	//need to encode schema
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(schema)
	if err != nil {
		return
	}
	//store the schema in the tar file
	data := buf.Bytes()
	if err := tw.WriteHeader(&tar.Header{
		Name:    "schema/schema.gob",
		Mode:    0666,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}); err != nil {
		return err
	}
	if _, err = tw.Write(data); err != nil {
		return
	}

	for _, index := range schema {
		for _, frame := range index.Frames {
			for _, view := range frame.Views {
				log.Println("Backing up:", index.Name, frame.Name, view.Name)
				if err = client.BackupTo(ctx, tw, index.Name, frame.Name, view.Name); err != nil {
					return
				}
			}

		}
	}
	tw.Close()
	gw.Flush()
	gw.Close()
	return
}

func (cmd *BackupCommand) TLSHost() string {
	return cmd.Host
}

func (cmd *BackupCommand) TLSConfiguration() pilosa.TLSConfig {
	return cmd.TLS
}
