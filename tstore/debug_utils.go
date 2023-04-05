package tstore

/*internal node
example SLOTROW-->  <TR><TD PORT="f0">10</TD><TD PORT="f1">20</TD><TD PORT="f2">30</TD></TR>
node0[ label =<
<table BORDER="0" CELLBORDER="1" CELLSPACING="0">
<TR><TD SIDES="LT" ALIGN="LEFT">PageID:</TD><TD SIDES="RT" COLSPAN="%v" ALIGN="RIGHT">%v</TD></TR>
<TR><TD SIDES="LB" ALIGN="LEFT">SlotCount:</TD><TD SIDES="RB" COLSPAN="%v" ALIGN="RIGHT">%v</TD></TR>
	  %v
      </table>>
      fillcolor="lightgrey" margin="0"];
*/
import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/featurebasedb/featurebase/v3/bufferpool"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/vprint"
	"github.com/featurebasedb/featurebase/v3/wireprotocol"
)

func internalNode(node *BTreeNode, out io.Writer) {
	si := bufferpool.NewPageSlotIterator(node.page, 0)
	slot := si.Next()
	numSlots := 0
	var slotRow strings.Builder
	// build slotrow
	slotRow.WriteString("<TR>")
	var linker strings.Builder
	nodeid := fmt.Sprintf("%v", node.page.ID().Page)
	for slot != nil {
		numSlots++
		pl := slot.KeyPayload(node.page)
		k := int(pl.KeyAsInt(node.page))
		ipl := slot.InternalPayload(node.page)
		pageID := bufferpool.PageID(ipl.ValueAsPagePointer(node.page))
		slotRow.WriteString(fmt.Sprintf(`<TD PORT="p%v">%v</TD>\n`, pageID.Page, k))
		linker.WriteString(fmt.Sprintf("\np%v:p%v->p%v;", nodeid, pageID.Page, pageID.Page))
		slot = si.Next()
	}
	pn := node.page.ReadNextPointer()
	if pn.Page != bufferpool.INVALID_PAGE {
		slotRow.WriteString(fmt.Sprintf(`<TD PORT="p%v">∅</TD>\n`, pn.Page))
		linker.WriteString(fmt.Sprintf("\np%v:p%v->p%v;", nodeid, pn.Page, pn.Page))
	}

	slotRow.WriteString("</TR>")

	fmt.Fprintf(out, `
	p%v[ label =<
<table BORDER="0" CELLBORDER="1" CELLSPACING="0">
<TR><TD SIDES="LT" ALIGN="LEFT">PageID:</TD><TD SIDES="RT" COLSPAN="%v" ALIGN="RIGHT">%v</TD></TR>
<TR><TD SIDES="LB" ALIGN="LEFT">SlotCount:</TD><TD SIDES="RB" COLSPAN="%v" ALIGN="RIGHT">%v</TD></TR>
	  %v
      </table>>
      fillcolor="lightgrey" margin="0"]`, node.page.ID().Page, numSlots, node.page.ID().Page, numSlots, numSlots, slotRow.String())
	out.Write([]byte(linker.String()))
}

func (b *BTree) leafNode(node *BTreeNode, out io.Writer, short bool, schema types.Schema) {
	sc := int16(node.slotCount())
	si := bufferpool.NewPageSlotIterator(node.page, 0)
	slot := si.Next()
	fmt.Fprintf(out, `
	p%v[label =<<table BORDER="0" CELLBORDER="1" CELLSPACING="0"> 
<TR><TD SIDES="LT" ALIGN="LEFT">PageID:</TD><TD SIDES="RT" COLSPAN="3" ALIGN="RIGHT">%v</TD></TR>
<TR><TD SIDES="LB" ALIGN="LEFT">SlotCount:</TD><TD SIDES="RB" COLSPAN="3" ALIGN="RIGHT">%v</TD></TR>`,
		node.page.ID().Page,
		node.page.ID().Page,
		sc)
	last := ""
	cnt := 0
	var overflowPages strings.Builder
	id := node.page.ID()
	for slot != nil {
		cnt++
		pl := slot.KeyPayload(node.page)
		k := int(pl.KeyAsInt(node.page))
		// Get Value // TODO(twg) 2023/03/28 (pok) better way todo this?
		lpl := slot.LeafPayload(node.page)
		rdr := lpl.GetPayloadReader(node.page)
		payload := make([]byte, rdr.PayloadTotalLength)
		copy(payload, rdr.PayloadChunkBytes)
		// check for overflow( rdr.Flags==1)
		if rdr.Flags == 1 {
			bytesReceived := rdr.PayloadChunkLength
			nextPtr := rdr.OverflowPtr
			fmt.Fprintf(out, "\n<TR><TD>%v</TD><TD PORT=\"p%v\" BGCOLOR=\"grey\" COLSPAN=\"3\">%v</TD></TR>", k, nextPtr, "OVERFLOW" /*payload*/)
			for nextPtr != bufferpool.INVALID_PAGE {
				onode, _ := b.fetchNode(bufferpool.PageID{ObjectID: id.ObjectID, Shard: id.Shard, Page: nextPtr})
				onode.takeReadLatch()
				// read the overflow bytes
				clen, cbytes := onode.page.ReadLeafPagePayloadBytes(bufferpool.PAGE_SLOTS_START_OFFSET)
				overflowPages.WriteString(fmt.Sprintf(`
				p%v[label=<<table BORDER="0" CELLBORDER="1" CELLSPACING="0">
			<TR><TD SIDES="LTB" ALIGN="LEFT">PageID:</TD><TD SIDES="RTB" ALIGN="RIGHT">%v</TD></TR>
			<TR><TD PORT="p%v" COLSPAN="3">%v</TD></TR>
			</table>>
			fillcolor="cornflowerblue" margin="0"];
			p%v:p%v->p%v;
			`, nextPtr, nextPtr, nextPtr, "cbytes", id.Page, nextPtr, nextPtr))
				copy(payload[bytesReceived:], cbytes)
				bytesReceived += clen

				nextPtr = onode.page.ReadNextPointer().Page
				onode.releaseReadLatch()
				b.unpin(onode)

			}
		} else {
			t := NewBTreeTupleFromBytes(payload, schema)
			values := make([]string, len(t.Tuple))
			types := make([]string, len(t.Tuple))
			names := make([]string, len(t.Tuple))
			for i := range t.Tuple {
				values[i] = fmt.Sprintf("%v", t.Tuple[i])
				types[i] = t.TupleSchema[i].Type.TypeDescription()
				names[i] = t.TupleSchema[i].ColumnName
				///
			}
			if short {
				if cnt < 2 {
					fmt.Fprintf(out, "\n<TR><TD>%v</TD><TD>%v</TD><TD>%v</TD><TD>%v</TD></TR>", k, strings.Join(names, "<BR/>"), strings.Join(types, "<BR/>"), strings.Join(values, "<BR/>"))
				} else {
					last = fmt.Sprintf("\n<TR><TD>%v</TD><TD>%v</TD><TD>%v</TD><TD>%v</TD></TR>", k, strings.Join(names, "<BR/>"), strings.Join(types, "<BR/>"), strings.Join(values, "<BR/>"))
				}
			} else {
				fmt.Fprintf(out, "\n<TR><TD>%v</TD><TD>%v</TD><TD>%v</TD><TD>%v</TD></TR>", k, strings.Join(names, "<BR/>"), strings.Join(types, "<BR/>"), strings.Join(values, "<BR/>"))
			}
		}
		slot = si.Next()
	}
	if short {
		fmt.Fprintf(out, "\n<TR><TD>...</TD><TD>...</TD><TD>...</TD><TD>...</TD></TR>")
		out.Write([]byte(last))
		fmt.Fprintln(out, "")
	}
	out.Write([]byte(`</table>> fillcolor="turquoise" margin="0"]`))
	op := overflowPages.String()
	if len(op) > 0 {
		out.Write([]byte(overflowPages.String()))
	}
}

func (b *BTree) Dot(out io.Writer, src string, short bool) {
	fmt.Fprintf(out, `digraph g {
				labelloc="t"
				labeljust="r"
				label=<<table border="0" cellspacing="0">
				   <tr><td align="left">BTREE:</td><td>%v</td></tr>
				   <tr><td align="left">keysPerLeafPage:</td><td>%v</td></tr>
				   <tr><td align="left">keysPerInternalPage:</td><td>%v</td></tr>
				</table>>
			rankdir="TB"
			node [shape = plaintext,height=.1, style=filled ];`, src, b.keysPerLeafPage, b.keysPerInternalPage)

	node, _ := b.fetchNode(b.rootNode)
	defer b.bufferpool.UnpinPage(node.page.ID())
	b.dot(node, out, short)
	fmt.Printf("}")
}

func (b *BTree) dot(node *BTreeNode, out io.Writer, short bool) {
	if node.isLeaf() {
		b.leafNode(node, out, short, b.schema)
	} else {
		internalNode(node, out)
		si := bufferpool.NewPageSlotIterator(node.page, 0)
		slot := si.Next()
		for slot != nil {
			ipl := slot.InternalPayload(node.page)
			pn := bufferpool.PageID(ipl.ValueAsPagePointer(node.page))
			cn, _ := b.fetchNode(pn)
			b.dot(cn, out, short)
			b.bufferpool.UnpinPage(cn.page.ID())
			slot = si.Next()
		}
		pn := node.page.ReadNextPointer()
		if pn.Page != bufferpool.INVALID_PAGE {
			cn, _ := b.fetchNode(pn)
			b.dot(cn, out, short)
			b.bufferpool.UnpinPage(cn.page.ID())
		}
	}
}

func OpenBtree(path string) (*BTree, error) {
	diskManager := bufferpool.NewTupleStoreDiskManager()
	objectID, shard := int32(0), int32(0) // these don't matter just to open the file
	diskManager.CreateOrOpenShard(objectID, shard, path)
	page0, err := diskManager.ReadPage(bufferpool.PageID{ObjectID: objectID, Shard: shard, Page: 0})
	if err != nil {
		return nil, err
	}
	//  ok so I have a page,now what todo with it?
	// need to find the schema, I think its in slot 1?
	slot := page0.ReadPageSlot(1)
	ipl := slot.InternalPayload(page0)
	schemaPageID := ipl.ValueAsPagePointer(page0)
	schemaPage, err := diskManager.ReadPage(schemaPageID)
	if err != nil {
		return nil, err
	}

	schema, err := getSchemaFrom(schemaPage)
	if err != nil {
		return nil, err
	}
	bufferPool := bufferpool.NewBufferPool(100, diskManager)
	return NewBTree(8, objectID, shard, schema, bufferPool)
}

func getSchemaFrom(page *bufferpool.Page) (types.Schema, error) {
	sc := page.ReadSlotCount()
	vprint.VV("slots:%v", sc)
	slot := page.ReadPageSlot(int16(1))
	lpl := slot.LeafPayload(page)
	rdr := lpl.GetPayloadReader(page)
	payload := make([]byte, rdr.PayloadTotalLength)
	copy(payload, rdr.PayloadChunkBytes)
	// this works as long as the schema doesn't overflow
	if rdr.Flags == 1 {
		panic("fix this")
	}
	schema := types.Schema{
		&types.PlannerColumn{
			ColumnName: "schema",
			Type:       parser.NewDataTypeVarbinary(16384),
		},
	}
	ss := NewBTreeTupleFromBytes(payload, schema)
	b := ss.Tuple[0].([]byte)
	rd := bytes.NewReader(b)
	_, err := wireprotocol.ExpectToken(rd, wireprotocol.TOKEN_SCHEMA_INFO)
	if err != nil {
		return nil, err
	}

	s, err := wireprotocol.ReadSchema(rd)
	if err != nil {
		return nil, err
	}
	return s, nil
}
