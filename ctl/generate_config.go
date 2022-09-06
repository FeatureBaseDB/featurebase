// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"context"
	"fmt"
	"io"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/server"
	"github.com/pelletier/go-toml"
	"github.com/pkg/errors"
)

// GenerateConfigCommand represents a command for printing a default config.
type GenerateConfigCommand struct {
	*pilosa.CmdIO
}

// NewGenerateConfigCommand returns a new instance of GenerateConfigCommand.
func NewGenerateConfigCommand(stdin io.Reader, stdout, stderr io.Writer) *GenerateConfigCommand {
	return &GenerateConfigCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run prints out the default config.
func (cmd *GenerateConfigCommand) Run(_ context.Context) error {
	conf := server.NewConfig()
	ret, err := toml.Marshal(*conf)
	if err != nil {
		return errors.Wrap(err, "unmarshalling default config")
	}
	fmt.Fprintf(cmd.Stdout, "%s\n", ret)
	return nil
}
