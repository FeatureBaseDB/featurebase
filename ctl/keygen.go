// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"fmt"
	"io"

	"github.com/gorilla/securecookie"
	pilosa "github.com/molecula/featurebase/v2"
)

// Keygen represents a command for generating crytographic keys.
type KeygenCommand struct {
	CmdIO     *pilosa.CmdIO
	KeyLength int
}

// NewKeygen returns a new instance of Keygen.
func NewKeygenCommand(stdin io.Reader, stdout, stderr io.Writer) *KeygenCommand {
	return &KeygenCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run keys to use for authentication .
func (kg *KeygenCommand) Run(_ context.Context) error {
	fmt.Printf("hash-key = \"%+x\"\n", securecookie.GenerateRandomKey(kg.KeyLength))
	fmt.Printf("block-key = \"%+x\"\n", securecookie.GenerateRandomKey(kg.KeyLength))
	return nil
}
