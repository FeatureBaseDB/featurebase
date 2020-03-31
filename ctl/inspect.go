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
	// don't list details of objects
	Quiet bool
	// list only this many objects
	Max int

	// Standard input/output
	*pilosa.CmdIO
}

// NewInspectCommand returns a new instance of InspectCommand.
func NewInspectCommand(stdin io.Reader, stdout, stderr io.Writer) *InspectCommand {
	return &InspectCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

type pointerContext struct {
	from, to uintptr
}

func (p *pointerContext) pretty(c roaring.ContainerInfo) string {
	var pointer string
	if c.Mapped {
		if c.Pointer >= p.from && c.Pointer < p.to {
			pointer = fmt.Sprintf("@+0x%x", c.Pointer-p.from)
		} else {
			pointer = fmt.Sprintf("!0x%x!", c.Pointer)
		}
	} else {
		pointer = fmt.Sprintf("0x%x", c.Pointer)
	}
	return fmt.Sprintf("%s \t%d \t%d \t%s ", c.Type, c.N, c.Alloc, pointer)
}

func (cmd *InspectCommand) PrintOps(info roaring.BitmapInfo) {
	fmt.Fprintln(cmd.Stdout, "== Ops ==")
	tw := tabwriter.NewWriter(cmd.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprintf(tw, "%s\t%s\t%s\t\n", "TYPE", "OpN", "SIZE")
	printed := 0
	for _, op := range info.OpDetails {
		fmt.Fprintf(tw, "%s\t%d\t%d\t\n", op.Type, op.OpN, op.Size)
		printed++
		if cmd.Max != 0 && printed >= cmd.Max {
			break
		}
	}
	tw.Flush()
}

func (cmd *InspectCommand) PrintContainers(info roaring.BitmapInfo, pC pointerContext) {
	fmt.Fprintln(cmd.Stdout, "== Containers ==")
	tw := tabwriter.NewWriter(cmd.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprintf(tw, "\tRoaring\t\t\t\tOps\t\t\t\t\n")
	fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n", "KEY", "TYPE", "N", "ALLOC", "OFFSET", "TYPE", "N", "ALLOC", "OFFSET")
	c1s := info.Containers
	c2s := info.OpContainers
	l1 := len(c1s)
	l2 := len(c2s)
	i1 := 0
	i2 := 0
	var c1, c2 roaring.ContainerInfo
	c1.Key = ^uint64(0)
	c2.Key = ^uint64(0)
	c1e := false
	c2e := false
	if i1 < l1 {
		c1 = c1s[i1]
		i1++
		c1e = true
	}
	if i2 < l2 {
		c2 = c2s[i2]
		i2++
		c2e = true
	}
	printed := 0
	for c1e || c2e {
		c1used := false
		c2used := false
		var key uint64
		c1fmt := "-\t\t\t"
		c2fmt := "-\t\t\t"
		if !c2e || (c1e && c1.Key < c2.Key) {
			c1fmt = pC.pretty(c1)
			key = c1.Key
			c1used = true
		} else if !c1e || (c2e && c2.Key < c1.Key) {
			c2fmt = pC.pretty(c2)
			key = c2.Key
			c2used = true
		} else {
			// c1e and c2e both set, and neither key is < the other.
			c1fmt = pC.pretty(c1)
			c2fmt = pC.pretty(c2)
			key = c1.Key
			c1used = true
			c2used = true
		}
		if c1used {
			if i1 < l1 {
				c1 = c1s[i1]
				i1++
			} else {
				c1e = false
			}
		}
		if c2used {
			if i2 < l2 {
				c2 = c2s[i2]
				i2++
			} else {
				c2e = false
			}
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t\n", key, c1fmt, c2fmt)
		printed++
		if cmd.Max > 0 && printed >= cmd.Max {
			break
		}
	}
	tw.Flush()
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
	fmt.Fprintf(cmd.Stderr, "inspecting bitmap...")
	info, err := roaring.InspectBinary(data)
	if err != nil {
		return errors.Wrap(err, "inspecting")
	}
	fmt.Fprintf(cmd.Stderr, " (%s)\n", time.Since(t))

	pC := pointerContext{
		from: uintptr(unsafe.Pointer(&data[0])),
	}
	pC.to = pC.from + uintptr(len(data))

	// Print top-level info.
	fmt.Fprintf(cmd.Stdout, "== Bitmap Info ==\n")
	fmt.Fprintf(cmd.Stdout, "Bits: %d\n", info.BitCount)
	fmt.Fprintf(cmd.Stdout, "Containers: %d (%d roaring)\n", info.ContainerCount, len(info.Containers))
	fmt.Fprintf(cmd.Stdout, "Operations: %d (%d bits)\n", info.Ops, info.OpN)
	fmt.Fprintln(cmd.Stdout, "")

	// Print info for each container.
	if !cmd.Quiet {
		if info.ContainerCount > 0 {
			cmd.PrintContainers(info, pC)
		}
		if info.Ops > 0 {
			cmd.PrintOps(info)
		}
	}

	return nil
}
