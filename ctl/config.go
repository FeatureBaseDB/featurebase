// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/featurebasedb/featurebase/v3/server"
	toml "github.com/pelletier/go-toml"
)

// ConfigCommand represents a command for printing a default config.
type ConfigCommand struct {
	// this exists so we can override it in tests
	stdout io.Writer
	// this actually gets overridden more generally by different tests
	stderr io.Writer
	Config *server.Config
}

// NewConfigCommand returns a new instance of ConfigCommand.
func NewConfigCommand(stderr io.Writer) *ConfigCommand {
	return &ConfigCommand{
		stdout: os.Stdout,
		stderr: stderr,
	}
}

// Run prints out the default config.
func (cmd *ConfigCommand) Run(_ context.Context) error {
	buf, err := toml.Marshal(*cmd.Config)
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.stdout, string(buf))
	return nil
}
