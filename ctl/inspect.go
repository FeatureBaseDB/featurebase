// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/pb"
	"github.com/molecula/featurebase/v3/roaring"
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
	// Filters:
	InspectOpts pilosa.InspectRequest

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
	fmt.Fprintln(cmd.Stdout, "  Ops:")
	tw := tabwriter.NewWriter(cmd.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprintf(tw, "  \t%s\t%s\t%s\t\n", "TYPE", "OpN", "SIZE")
	printed := 0
	for _, op := range info.OpDetails {
		fmt.Fprintf(tw, "\t%s\t%d\t%d\t\n", op.Type, op.OpN, op.Size)
		printed++
		if cmd.Max != 0 && printed >= cmd.Max {
			break
		}
	}
	tw.Flush()
}

func (cmd *InspectCommand) PrintContainers(info roaring.BitmapInfo, pC pointerContext) {
	fmt.Fprintln(cmd.Stdout, "  Containers:")
	tw := tabwriter.NewWriter(cmd.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprintf(tw, "  \t\tRoaring\t\t\t\tOps\t\t\t\tFlags\t\n")
	fmt.Fprintf(tw, "\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n", "KEY", "TYPE", "N", "ALLOC", "OFFSET", "TYPE", "N", "ALLOC", "OFFSET", "FLAGS")
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
		// If c2 exists, we'll always prefer its flags,
		// if it doesn't, this gets overwritten.
		flags := c2.Flags
		if !c2e || (c1e && c1.Key < c2.Key) {
			c1fmt = pC.pretty(c1)
			key = c1.Key
			c1used = true
			flags = c1.Flags
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
		fmt.Fprintf(tw, "\t%d\t%s\t%s\t%s\t\n", key, c1fmt, c2fmt, flags)
		printed++
		if cmd.Max > 0 && printed >= cmd.Max {
			break
		}
	}
	tw.Flush()
}

// Run executes the inspect command.
func (cmd *InspectCommand) Run(ctx context.Context) error {
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
	if fi.IsDir() {
		total := 0
		infos, err := f.Readdir(0)
		if err != nil {
			return err
		}
		if len(infos) == 0 {
			return errors.New("directory contains no files")
		}

		names := make([]string, len(infos))
		nameToInfo := make(map[string]os.FileInfo, len(infos))
		// find numeric-only names; we'll operate on
		// either those, or the whole holder if we find
		// a .topology file.
		n := 0
		for _, fi := range infos {
			name := fi.Name()
			if name == ".topology" {
				return cmd.InspectHolder(ctx, cmd.Path)
			}
			if _, err := strconv.Atoi(name); err == nil {
				names[n] = name
				nameToInfo[name] = fi
				n++
			}
		}
		if n == 0 {
			return fmt.Errorf("directory contains no fragments (looking for numeric names)")
		}
		names = names[:n]
		fmt.Fprintf(cmd.Stdout, "%s contains %d fragments:\n", cmd.Path, n)
		for _, name := range names {
			f2, err := os.Open(filepath.Join(cmd.Path, name))
			if err != nil {
				return fmt.Errorf("opening %q: %v", name, err)
			}
			fmt.Fprintf(cmd.Stdout, "%s/%s:\n", cmd.Path, name)
			err = cmd.InspectFile(f2, nameToInfo[name])
			total++
			f2.Close()
			if err != nil {
				return fmt.Errorf("inspecting %q: %v", name, err)
			}
		}
		return nil
	}
	return cmd.InspectFile(f, fi)
}

// loadTopology is copied almost exactly from pilosa/cluster.go.
func loadTopology(path string) (topology pb.Topology, myID string, err error) {
	buf, err := ioutil.ReadFile(filepath.Join(path, ".topology"))
	if os.IsNotExist(err) {
		return topology, myID, err
	} else if err != nil {
		return topology, myID, errors.Wrap(err, "reading file")
	}
	if err := proto.Unmarshal(buf, &topology); err != nil {
		return topology, myID, errors.Wrap(err, "unmarshalling")
	}
	sort.Slice(topology.NodeIDs,
		func(i, j int) bool {
			return topology.NodeIDs[i] < topology.NodeIDs[j]
		})
	buf, err = ioutil.ReadFile(filepath.Join(path, ".id"))
	if os.IsNotExist(err) {
		return topology, myID, err
	} else if err != nil {
		return topology, myID, nil
	}
	myID = strings.TrimSpace(string(buf))
	return topology, myID, nil
}

var partitions = make(map[string]map[uint64]int)

func findPartition(index string, shard uint64, partitionN int) (partition int) {
	var shardMap map[uint64]int
	var ok bool
	if shardMap, ok = partitions[index]; !ok {
		shardMap = make(map[uint64]int)
		partitions[index] = shardMap
	}
	if partition, ok = shardMap[shard]; !ok {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], shard)

		// Hash the bytes and mod by partition count.
		h := fnv.New64a()
		_, _ = h.Write([]byte(index))
		_, _ = h.Write(buf[:])
		partition = int(h.Sum64() % uint64(partitionN))
		shardMap[shard] = partition
	}
	return partition
}

func findPartitionPath(path string, partitionN int) (int, error) {
	parts := strings.Split(path, "/")
	shard, err := strconv.ParseUint(parts[len(parts)-1], 10, 64)
	if err != nil {
		return 0, err
	}
	return findPartition(parts[0], shard, partitionN), nil
}

func (cmd *InspectCommand) InspectHolder(ctx context.Context, path string) error {
	holder := pilosa.NewHolder(path, nil)
	holder.Opts.Inspect = true
	holder.Opts.ReadOnly = true
	err := holder.Open()
	if err != nil {
		return fmt.Errorf("%s: holder open: %v", path, err)
	}
	holderInfo, err := holder.Inspect(ctx, &cmd.InspectOpts)
	if err != nil {
		return fmt.Errorf("%s: inspect: %v", path, err)
	}
	myPartition := 0
	topology, myID, err := loadTopology(path)
	if err == nil {
		fmt.Fprintf(cmd.Stdout, "Cluster ID: %q\n", topology.ClusterID)
		if len(topology.NodeIDs) > 1 {
			fmt.Fprintf(cmd.Stdout, "Cluster of %d nodes, this node %q\n", len(topology.NodeIDs), myID)
		} else {
			fmt.Fprintf(cmd.Stdout, "Cluster has only one node: %q\n", myID)
		}
		found := false
		for i := range topology.NodeIDs {
			if topology.NodeIDs[i] == myID {
				found = true
				myPartition = i
				break
			}
		}
		if !found {
			fmt.Fprintf(cmd.Stdout, "Warning: node ID %q not found in topology (%q)\n", myID, topology.NodeIDs)
		}
	} else {
		fmt.Fprintf(cmd.Stdout, "warning: reading topology failed: %v\n", err)
	}
	for _, name := range holderInfo.FragmentNames {
		partition, err := findPartitionPath(name, len(topology.NodeIDs))
		if err != nil {
			fmt.Fprintf(cmd.Stdout, "%s: [can't find partition: %v]\n", name, err)
		} else {
			if partition == myPartition {
				fmt.Fprintf(cmd.Stdout, "%s:\n", name)
			} else {
				fmt.Fprintf(cmd.Stdout, "%s: [primary node %q]\n", name, topology.NodeIDs[partition])
			}
		}
		details := holderInfo.FragmentInfo[name]
		cmd.DisplayInfo(details.BitmapInfo)
		if details.BlockChecksums != nil {
			fmt.Fprintf(cmd.Stdout, "  Checksums [%d total]:\n", len(details.BlockChecksums))
			for _, block := range details.BlockChecksums {
				fmt.Fprintf(cmd.Stdout, "   %8d: %x\n", block.ID, block.Checksum)
			}
		}
	}
	return nil
}

func (cmd *InspectCommand) InspectFile(f *os.File, fi os.FileInfo) error {
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
	mappedFrom := uintptr(unsafe.Pointer(&data[0]))
	mappedTo := mappedFrom + uintptr(len(data))
	// Attach the mmap file to the bitmap.
	t := time.Now()
	fmt.Fprintf(cmd.Stderr, "inspecting bitmap...")
	var info roaring.BitmapInfo
	bitmap, _, err := roaring.InspectBinary(data, true, &info)
	fmt.Fprintf(cmd.Stderr, " (%s)\n", time.Since(t))
	cmd.DisplayInfo(info)
	if err != nil {
		return errors.Wrap(err, "inspecting")
	}
	mappedIn, mappedOut, unmappedIn, errs, err := bitmap.SanityCheckMapping(mappedFrom, mappedTo)
	if err != nil {
		fmt.Fprintf(cmd.Stderr, "sanity check: %d mapped in, %d mapped out, %d unmapped in, %d errors\n",
			mappedIn, mappedOut, unmappedIn, errs)
		fmt.Fprintf(cmd.Stderr, "last error: %v\n", err)
	}
	return nil
}

func (cmd *InspectCommand) DisplayInfo(info roaring.BitmapInfo) {
	pC := pointerContext{
		from: info.From,
		to:   info.To,
	}

	// Print top-level info.
	fmt.Fprintf(cmd.Stdout, "  Bitmap Info:\n")
	fmt.Fprintf(cmd.Stdout, "    Bits: %d\n", info.BitCount)
	fmt.Fprintf(cmd.Stdout, "    Containers: %d (%d roaring)\n", info.ContainerCount, len(info.Containers))
	fmt.Fprintf(cmd.Stdout, "    Operations: %d (%d bits)\n", info.Ops, info.OpN)
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
}
