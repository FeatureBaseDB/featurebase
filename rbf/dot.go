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
	"fmt"
	"io"
)

func dotCell(b []byte, parent string, writer io.Writer) {
	pgno := readPageNo(b)
	if pgno == Magic32() {
		fmt.Fprintf(writer, "==META\n")
		return
	}

	flags := readFlags(b)
	cellN := readCellN(b)

	switch {
	case flags&PageTypeLeaf != 0:
		fmt.Fprintf(writer, "cell%d [ shape=none label=<<table border=\"0\" cellspacing=\"0\">\n", pgno)
		fmt.Fprintf(writer, "<tr><td border=\"1\">CELL (%d) </td></tr>\n", pgno)
		links := make([]string, 0)
		for i := 0; i < cellN; i++ {
			cell := readLeafCell(b, i)
			switch cell.Type {
			case ContainerTypeArray:
				//fmt.Fprintf(os.Stderr, "[%d]: key=%d type=array n=%d elems=%v\n", i, cell.Key, cell.N, toArray16(cell.Data))
				fmt.Fprintf(writer, "<tr><td border=\"1\" bgcolor=\"green\"><font color=\"white\">[%d]: key=%d type=array n=%d</font></td></tr>\n", i, cell.Key, cell.BitN)
			case ContainerTypeRLE:
				fmt.Fprintf(writer, "<tr><td border=\"1\" bgcolor=\"blue\"><font color=\"white\">[%d]: key=%d type=rle n=%d</font></td></tr>\n", i, cell.Key, cell.BitN)
			case ContainerTypeBitmapPtr:
				bpn := toPgno(cell.Data)
				fmt.Fprintf(writer, "<tr><td border=\"1\" bgcolor=\"red\" port=\"%d\"><font color=\"white\">[%d]: key=%d type=bitmap n=%d </font></td></tr>\n", bpn, i, cell.Key, cell.BitN)
				links = append(links, fmt.Sprintf("bitmap%d[label=\"bitmap (%d)\"]\n cell%d:%d -> bitmap%d\n", bpn, bpn, pgno, i, bpn))
			default:
				fmt.Fprintf(writer, "<tr><td border=\"1\" bgcolor=\"yellow\">[%d]: key=%d type=unknown<%d> n=%d</td></tr>\n", i, cell.Key, cell.Type, cell.BitN)
			}
		}
		fmt.Fprintf(writer, "</table>>]\n")
		fmt.Fprintf(writer, "%s -> cell%d\n", parent, pgno)
		for _, link := range links {
			fmt.Fprintf(writer, "%s", link)
		}
	default:
		//should not happen
		fmt.Fprintf(writer, "==!PAGE %d flags=%d\n", pgno, flags)
	}
}

// Dumpdot recursively writes the tree representation starting from a given page to STDERR.
func Dumpdot(tx *Tx, pgno uint32, parent string, writer io.Writer) {
	page, _, err := tx.readPage(pgno)
	if err != nil {
		panic(err)
	}

	if IsMetaPage(page) {
		//fmt.Fprintf(writer, "META(%d)\n", pgno)
		//fmt.Fprintf(writer, "└── <FREELIST>\n")
		//treedump(tx, readMetaFreelistPageNo(page), indent+"    ")

		visitor := func(pgno uint32, records []*RootRecord) {
			rr := fmt.Sprintf("rr%d", pgno)
			fmt.Fprintf(writer, "%s[label=\"ROOT RECORD(%d): n=%d\"]\n", rr, pgno, len(records))
			for _, record := range records {
				root := fmt.Sprintf("root%d", record.Pgno)
				fmt.Fprintf(writer, "%s[label=\"ROOT(%d)| %s\"]\n%s->%s\n", root, record.Pgno, record.Name, rr, root)
				p := fmt.Sprintf("root%d", record.Pgno)
				Dumpdot(tx, record.Pgno, p, writer)

			}
		}
		Walk(tx, readMetaRootRecordPageNo(page), visitor)

		return
	}

	// Handle
	switch typ := readFlags(page); typ {
	case PageTypeBranch:
		p := fmt.Sprintf("branch%d", pgno)
		fmt.Fprintf(writer, "%s[label=\"BRANCH(%d)| n=%d\"]\n %s->%s\n", p, pgno, readCellN(page), parent, p)
		for i, n := 0, readCellN(page); i < n; i++ {
			cell := readBranchCell(page, i)
			if cell.Flags&uint32(ContainerTypeBitmap) == 0 { // leaf/branch child page
				Dumpdot(tx, cell.ChildPgno, p, writer)
			} else {
				b := fmt.Sprintf("bm%d", cell.ChildPgno)
				fmt.Fprintf(writer, "%s[label=\"BITMAP(%d) key=%d \"]\n %s -> %s\n", b, cell.ChildPgno, cell.LeftKey, p, b)
			}
		}
	case PageTypeLeaf:
		p := fmt.Sprintf("leaf%d", pgno)
		fmt.Fprintf(writer, "%s[label=\"LEAF(%d)| n=%d\"]\n%s->%s\n", p, pgno, readCellN(page), parent, p)
		dotCell(page, p, writer)
	}
}
