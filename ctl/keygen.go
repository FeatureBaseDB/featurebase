// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/gorilla/securecookie"
)

// KeygenCommand represents a command for generating a cryptographic key.
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
