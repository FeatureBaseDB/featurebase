// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/pkg/errors"
)

// CheckCommand represents a command for performing consistency checks on data files.
type CheckCommand struct {
	// Data file paths.
	Paths []string

	// Standard input/output
	*pilosa.CmdIO
}

// NewCheckCommand returns a new instance of CheckCommand.
func NewCheckCommand(stdin io.Reader, stdout, stderr io.Writer) *CheckCommand {
	return &CheckCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run executes the check command.
func (cmd *CheckCommand) Run(_ context.Context) error {
	for _, path := range cmd.Paths {
		switch filepath.Ext(path) {
		case "":
			if err := cmd.checkBitmapFile(path); err != nil {
				return errors.Wrap(err, "checking bitmap")
			}

		case ".cache":
			if err := cmd.checkCacheFile(path); err != nil {
				return errors.Wrap(err, "checking cache")
			}

		case ".snapshotting":
			if err := cmd.checkSnapshotFile(path); err != nil {
				return errors.Wrap(err, "checking snapshot")
			}
		}
	}

	return nil
}

// checkBitmapFile performs a consistency check on path for a roaring bitmap file.
func (cmd *CheckCommand) checkBitmapFile(path string) (err error) {
	// Open file handle.
	f, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "opening file")
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return errors.Wrap(err, "statting file")
	}

	// Memory map the file.
	data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return errors.Wrap(err, "mmapping")
	}
	defer func() {
		e := syscall.Munmap(data)
		if e != nil {
			fmt.Fprintf(cmd.Stderr, "WARNING: munmap failed: %v", e)
		}
		// don't overwrite another error with this, but also indicate
		// this error.
		if err == nil {
			err = e
		}
	}()
	// Attach the mmap file to the bitmap.
	bm := roaring.NewBitmap()
	if err := bm.UnmarshalBinary(data); err != nil {
		return errors.Wrap(err, "unmarshalling")
	}

	// Perform consistency check.
	if err := bm.Check(); err != nil {
		// Print returned errors.
		switch err := err.(type) {
		case roaring.ErrorList:
			for i := range err {
				fmt.Fprintf(cmd.Stdout, "%s: %s\n", path, err[i].Error())
			}
		default:
			fmt.Fprintf(cmd.Stdout, "%s: %s\n", path, err.Error())
		}
	}

	// Print success message if no errors were found.
	fmt.Fprintf(cmd.Stdout, "%s: ok\n", path)

	return nil
}

// checkCacheFile performs a consistency check on path for a cache file.
func (cmd *CheckCommand) checkCacheFile(path string) error {
	fmt.Fprintf(cmd.Stderr, "%s: ignoring cache file\n", path)
	return nil
}

// checkSnapshotFile performs a consistency check on path for a snapshot file.
func (cmd *CheckCommand) checkSnapshotFile(path string) error {
	fmt.Fprintf(cmd.Stderr, "%s: ignoring snapshot file\n", path)
	return nil
}
