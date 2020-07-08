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
package rbf

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/pilosa/pilosa/v2/roaring"
)

//probably should just implement the container interface
// but for now i'll do it
func (c *Cursor) Rows() ([]uint64, error) {
	shardVsContainerExponent := uint(4) //needs constant exported from roaring package
	if err := c.First(); err != nil {
		return nil, err
	}
	rows := make([]uint64, 0)
	var err error
	var lastRow uint64 = math.MaxUint64
	for {
		err := c.Next()
		if err != nil {
			break
		}
		cell := c.cell()
		vRow := cell.Key >> shardVsContainerExponent
		if vRow == lastRow {
			continue
		}
		rows = append(rows, vRow)
		lastRow = vRow
	}
	return rows, err
}

func (tx *Tx) FieldViews() []string {
	r, _ := tx.rootRecords()
	res := make([]string, len(r))
	for i := range r {
		res[i] = r[i].Name
	}
	return res
}

func (c *Cursor) DumpKeys() error {
	if err := c.First(); err != nil {
		return err
	}
	for {
		err := c.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		cell := c.cell()
		fmt.Println("key", cell.Key)
	}
}

func (c *Cursor) DumpStack() {
	fmt.Println("STACK")
	for i := c.stack.index; i >= 0; i-- {
		fmt.Printf("%+v\n", c.stack.elems[i])
	}
	fmt.Println()
}

func (c *Cursor) Dump() {
	bufStdout := bufio.NewWriter(os.Stdout)
	defer bufStdout.Flush()
	fmt.Fprintf(bufStdout, "digraph RBF{\n")
	fmt.Fprintf(bufStdout, "rankdir=\"LR\"\n")

	fmt.Fprintf(bufStdout, "node [shape=record height=.1]\n")
	dumpdot(c.tx, 0, " ", bufStdout)
	fmt.Fprintf(bufStdout, "\n}")
}

func (c *Cursor) Row(rowID uint64) (*roaring.Bitmap, error) {
	base := rowID * ShardWidth

	offset := uint64(c.tx.db.Shard * ShardWidth)
	off := highbits(offset)
	hi0, hi1 := highbits(base), highbits((rowID+1)*ShardWidth)
	c.stack.index = 0
	ok, err := c.Seek(hi0)
	if err != nil {
		return nil, err
	}
	if !ok {
		elem := &c.stack.elems[c.stack.index]
		n := readCellN(c.leafPage)
		if elem.index >= n {
			if err := c.goNextPage(); err != nil {
				return nil, err
			}
		}
	}
	other := roaring.NewSliceBitmap()
	for {
		err := c.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		cell := c.cell()
		if cell.Key >= hi1 {
			break
		}
		other.Containers.Put(off+(cell.Key-hi0), toContainer(cell))
	}
	return other, nil
}

// CurrentPageType returns the type of the container currently pointed to by cursor used in testing
// sometimes the cursor needs to be positions prior to this call with First/Last etc.
func (c *Cursor) CurrentPageType() int {
	cell := c.cell()
	return cell.Type
}

func toContainer(l leafCell) *roaring.Container {
	switch l.Type {
	case ContainerTypeArray:
		return roaring.NewContainerArray(toArray16(l.Data))
	case ContainerTypeBitmap:
		return roaring.NewContainerBitmap(l.N, toArray64(l.Data))
	case ContainerTypeRLE:
		return roaring.NewContainerRun(toInterval16(l.Data))
	}
	return nil
}
