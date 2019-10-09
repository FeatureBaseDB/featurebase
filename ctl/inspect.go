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
	"context"
	"fmt"
	"io"
	"os"
	"syscall"
	"text/tabwriter"
	"time"
	"unsafe"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pkg/errors"
)

// InspectCommand represents a command for inspecting fragment data files.
type InspectCommand struct {
	// Path to data file
	Path string

	// Standard input/output
	*pilosa.CmdIO
}

// NewInspectCommand returns a new instance of InspectCommand.
func NewInspectCommand(stdin io.Reader, stdout, stderr io.Writer) *InspectCommand {
	return &InspectCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run executes the inspect command.
func (cmd *InspectCommand) Run(_ context.Context) error {
	// Open file handle.
	f, err := os.Open(cmd.Path)
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
		err := syscall.Munmap(data)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "inspect command: munmap failed: %v", err)
		}
	}()
	// Attach the mmap file to the bitmap.
	t := time.Now()
	fmt.Fprintf(cmd.Stderr, "unmarshalling bitmap...")
	bm := roaring.NewBitmap()
	if err := bm.UnmarshalBinary(data); err != nil {
		return errors.Wrap(err, "unmarshalling")
	}
	fmt.Fprintf(cmd.Stderr, " (%s)\n", time.Since(t))

	// Retrieve stats.
	t = time.Now()
	fmt.Fprintf(cmd.Stderr, "calculating stats...")
	info := bm.Info()
	fmt.Fprintf(cmd.Stderr, " (%s)\n", time.Since(t))

	// Print top-level info.
	fmt.Fprintf(cmd.Stdout, "== Bitmap Info ==\n")
	fmt.Fprintf(cmd.Stdout, "Containers: %d\n", len(info.Containers))
	fmt.Fprintf(cmd.Stdout, "Operations: %d\n", info.OpN)
	fmt.Fprintln(cmd.Stdout, "")

	// Print info for each container.
	fmt.Fprintln(cmd.Stdout, "== Containers ==")
	tw := tabwriter.NewWriter(cmd.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprintf(tw, "%s\t%s\t% 8s \t% 8s\t%s\n", "KEY", "TYPE", "N", "ALLOC", "OFFSET")
	for _, ci := range info.Containers {
		fmt.Fprintf(tw, "%d\t%s\t% 8d \t% 8d \t0x%08x\n",
			ci.Key,
			ci.Type,
			ci.N,
			ci.Alloc,
			uintptr(ci.Pointer)-uintptr(unsafe.Pointer(&data[0])),
		)
	}
	tw.Flush()

	return nil
}
