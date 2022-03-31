package rbf

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
)

func (tx *Tx) Viz(w io.Writer) error {
	b := &builder{Writer: w}
	b.start()
	defer b.finish()

	roots, err := tx.RootRecords()
	if err != nil {
		return errors.Wrap(err, "root records")
	}

	itr := roots.Iterator()

	// we use page numbers for graphviz node IDs, but for cells we
	// need something different so we start very high assuming it
	// won't overlap
	cid := &cellID{id: 1 << 48}

	for name, pgno := itr.Next(); name != nil; name, pgno = itr.Next() {
		err := tx.walkTree(pgno.(uint32), 0, func(pgno, parent, typ uint32, err error) error {
			page, _, err := tx.readPage(pgno)
			if err != nil {
				return errors.Wrap(err, "reading page")
			}
			node := &vizNode{
				pgno: pgno,
				typ:  PageType(typ).String(),
			}
			if parent == 0 {
				node.name = name.(string)
			}
			node.cellN = readCellN(page)
			switch typ {
			case PageTypeBitmap:
				return nil
			case PageTypeLeaf:
				numBitmaps := 0
				for i, n := 0, readCellN(page); i < n; i++ {
					if cell := readLeafCell(page, i); cell.Type == ContainerTypeBitmapPtr {
						numBitmaps++
					} else {
						nodeid := cid.Next()
						b.addCell(&cell, nodeid)
						b.addEdge(int(pgno), nodeid)
					}

				}
				node.numBitmaps = numBitmaps
			}
			b.addNode(node)
			if parent != 0 {
				b.addEdge(int(parent), int(pgno))
			}
			return nil
		})
		if err != nil {
			return errors.Wrapf(err, "walking %s", name.(string))
		}
	}
	return nil
}

type cellID struct {
	id int
}

func (c *cellID) Next() int {
	c.id++
	return c.id
}

type vizNode struct {
	name       string
	pgno       uint32
	typ        string
	cellN      int
	numBitmaps int
}

// builder wraps an io.Writer and understands how to compose DOT formatted elements.
type builder struct {
	io.Writer
}

// start generates a title and initial node in DOT format.
func (b *builder) start() {
	graphname := "unnamed"
	fmt.Fprintln(b, `digraph "`+graphname+`" {`)
	fmt.Fprintln(b, `node [style=filled fillcolor="#f8f8f8"]`)
}

// finish closes the opening curly bracket in the constructed DOT buffer.
func (b *builder) finish() {
	fmt.Fprintln(b, "}")
}

// addNode generates a graph node in DOT format.
func (b *builder) addNode(node *vizNode) {
	label := fmt.Sprintf("%s %s", node.name, node.typ)
	if node.typ != "bitmap" {
		label = label + fmt.Sprintf(" N=%d", node.cellN)
	}
	if node.numBitmaps > 0 {
		label = label + fmt.Sprintf(" bitMapCells=%d", node.numBitmaps)
	}

	// Create DOT attribute for node.
	attr := fmt.Sprintf(`label="%s" id="node%d" shape="rectangle"`,
		label, node.pgno)

	fmt.Fprintf(b, "N%d [%s]\n", node.pgno, attr)
}

// addEdge generates a graph edge in DOT format.
func (b *builder) addEdge(from, to int) {
	fmt.Fprintf(b, "N%d -> N%d []\n", from, to)
}

func (b *builder) addCell(cell *leafCell, id int) {
	label := fmt.Sprintf(`k%d type:%s elemn:%d bitn:%d`, cell.Key, cell.Type, cell.ElemN, cell.BitN)

	fmt.Fprintf(b, `N%d [label="%s" id="node%d" shape="rectangle"]`, id, label, id)
}
