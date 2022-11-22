// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/gorilla/securecookie"
	"github.com/molecula/featurebase/v3/logger"
)

// Keygen represents a command for generating a cryptographic key.
type KeygenCommand struct {
	stdout    io.Writer
	logDest   logger.Logger
	KeyLength int
}

// NewKeygen returns a new instance of Keygen.
func NewKeygenCommand(logdest logger.Logger) *KeygenCommand {
	return &KeygenCommand{
		stdout:  os.Stdout,
		logDest: logdest,
	}
}

// Run keygen to obtain key to use for authentication .
func (kg *KeygenCommand) Run(_ context.Context) error {
	fmt.Fprintf(kg.stdout, "secret-key = \"%+x\"\n", securecookie.GenerateRandomKey(kg.KeyLength))
	return nil
}
