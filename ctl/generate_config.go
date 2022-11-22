// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/server"
	"github.com/pelletier/go-toml"
	"github.com/pkg/errors"
)

// GenerateConfigCommand represents a command for printing a default config.
type GenerateConfigCommand struct {
	stdout  io.Writer
	logDest logger.Logger
}

// NewGenerateConfigCommand returns a new instance of GenerateConfigCommand.
func NewGenerateConfigCommand(logdest logger.Logger) *GenerateConfigCommand {
	return &GenerateConfigCommand{
		stdout:  os.Stdout,
		logDest: logdest,
	}
}

// Run prints out the default config.
func (cmd *GenerateConfigCommand) Run(_ context.Context) error {
	conf := server.NewConfig()
	ret, err := toml.Marshal(*conf)
	if err != nil {
		return errors.Wrap(err, "unmarshalling default config")
	}
	fmt.Fprintf(cmd.stdout, "%s\n", ret)
	return nil
}
