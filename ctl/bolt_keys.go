// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/featurebasedb/featurebase/v3/logger"
	bolt "go.etcd.io/bbolt"
)

type BoltKeysCommand struct {
	// Filepath to the bolt database.
	Path string

	Hexa bool

	// Standard input/output
	stdout  io.Writer
	logDest logger.Logger
}

// NewRBFCheckCommand returns a new instance of RBFCheckCommand.
func NewBoltKeysCommand(logdest logger.Logger) *BoltKeysCommand {
	return &BoltKeysCommand{
		stdout:  os.Stdout,
		logDest: logdest,
	}
}

// The pilosa package makes these vars unexportable
// Copying and pasting these from translate_boltdb.go
// It may be useful to check on these constants in that file
// if something isn't working as expected
var (
	// ErrTranslateStoreClosed is returned when reading from an TranslateEntryReader
	// and the underlying store is closed.
	ErrBoltTranslateStoreClosed = errors.New("boltdb: translate store closing")

	// ErrTranslateKeyNotFound is returned when translating key
	// and the underlying store returns an empty set
	ErrTranslateKeyNotFound = errors.New("boltdb: translating key returned empty set")

	bucketKeys = []byte("keys")
	bucketIDs  = []byte("ids")
	bucketFree = []byte("free")
	freeKey    = []byte("free")
)

// Run executes a consistency check of an RBF database.
func (cmd *BoltKeysCommand) Run(ctx context.Context) error {

	db, err := bolt.Open(cmd.Path, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys

		for _, bucket := range [][]byte{bucketKeys, bucketIDs, bucketFree} {
			fmt.Fprintf(cmd.stdout, "Checking for keys in bucket named %s...\n", string(bucket))
			b := tx.Bucket(bucket)
			if b == nil {
				fmt.Fprintf(cmd.stdout, "Unable to find a bucket named %s...\n", string(bucket))
				continue
			}

			c := b.Cursor()

			if cmd.Hexa {
				for k, v := c.First(); k != nil; k, v = c.Next() {
					fmt.Printf("key=%x, value=%x\n", k, v)
				}
			} else {
				for k, v := c.First(); k != nil; k, v = c.Next() {
					fmt.Printf("key=%s, value=%s\n", k, v)
				}
			}

		}

		return nil
	})

	return nil
}
