package querycontext

import (
	"fmt"
	"io"
)

// Rendering time!
// We want to be able to render an rbfTxStore, or a QueryContext, or the whole system.
//
// Here's the things we have:
// rbfDBQueryContexts
// rbfTxStore
// rbfTxWrappers
// rbfQueryContext
// rbfQueryRead
// rbfQueryWrite
// rbf.DB
// rbf.Tx
// QueryScope
//
// Relationships that exist:
// rbfTxStore -> map[dbKey]rbfDBQueryContexts -> {one rbf.DB, map[rbfTxWrappers]rbfQueryContext}
// rbfQueryContext -> many rbfTxWrappers -> {one rbf.Tx, one rbfDBQueryContext, map[txKey]QueryRead}
// rbfQueryRead -> rbfTxWrappers
//
// For a TxStore, it's useful to graph the set of rbfDBQueryContexts it
// has (which represent live databases), and the QueryContexts associated
// with them. For a QueryContext, it's useful to graph the set of
// rbfTxWrappers it has, and also its scope.
//
// It is also interesting to graph the whole thing. There should always
// be a round trip in these relations; an rbfTxWrapper always maps to
// an rbfDBQueryContext, which should always have a reference back to
// the rbfQueryContext that owns the rbfTxWrapper. Similarly, if
// you have an rbfDBQueryContext, every rbfTxWrapper it references
// has a Tx against the associated DB.

// colors: ['#d7191c','#fdae61','#ffffbf','#abd9e9','#2c7bb6'] is a
// colorblind-friendly palette obtained from ColorBrewer. We assign
// meanings:
const (
	colorBlack   = "#000000"
	colorWhite   = "#ffffff"
	colorWrite   = "#fdae61"
	colorRead    = "#abd9e9"
	colorError   = "#d7191c"
	colorUnused1 = "#ffffbf"
	colorUnused2 = "##2c7bb6"
)

var colorWritable = map[bool]string{
	false: colorRead,
	true:  colorWrite,
}

type dotNode interface {
	writeNode(io.Writer)
	addTo(*dotGraph)
	dotId() string
	dotClass() string
}

type dotEdge interface {
	writeEdge(io.Writer, *dotGraph)
}

// dotGraph stores a list of nodes and edges it's found so far.
// we also have a queue. the reason is that if you don't, descent
// is depth-first, and this can lead to finding a node with very
// little depth left before you would have found it with a higher
// depth, and potentially not recording its edges.
type dotGraph struct {
	depth        int
	queue        []dotNode
	edges        []dotEdge
	nodes        map[dotNode]string
	orderedNodes []dotNode
	clusters     map[string][]dotNode
}

func (dg *dotGraph) enqueue(n dotNode) {
	if _, ok := dg.nodes[n]; ok {
		return
	}
	dg.queue = append(dg.queue, n)
}

// build starts with the enqueued roots and processes
// them, looking for new things, depth times.
func (dg *dotGraph) build(depth int) {
	if dg.nodes == nil {
		dg.nodes = make(map[dotNode]string)
	}
	dg.depth = depth
	for len(dg.queue) > 0 && dg.depth > 0 {
		working := dg.queue
		dg.queue = []dotNode{}
		for _, node := range working {
			node.addTo(dg)
		}
		dg.depth--
	}
}

// consider checks whether we're sufficiently done with this item, which
// means either we've already seen it, or we haven't but we're out of depth.
func (dg *dotGraph) consider(n dotNode) bool {
	if _, ok := dg.nodes[n]; ok {
		return true
	}
	dg.nodes[n] = n.dotId()
	// maintain a list in the order we saw them, so items at the top of
	// a given list will correspond to items at the top of the next list too
	dg.orderedNodes = append(dg.orderedNodes, n)
	return dg.depth < 1
}

var nodeQualities = map[string][2]map[string]string{
	"rdqc":    {{"rank": "same"}, {"shape": "cylinder"}},
	"txw":     {{"rank": "same"}, {"shape": "folder"}},
	"qrw":     {{"rank": "same"}, {"shape": "note"}},
	"txstore": {{"rank": "same"}, {"shape": "hexagon"}},
	"rqc":     {{"rank": "same"}, {"shape": "box3d"}},
}

var nodeOrder = []string{"txstore", "rdqc", "txw", "qrw", "rqc"}

func (dg *dotGraph) WriteClass(w io.Writer, class string) error {
	nodes := dg.clusters[class]
	if len(nodes) == 0 {
		return nil
	}
	fmt.Fprintf(w, "\tsubgraph %s {\n", class)
	if qualities, ok := nodeQualities[class]; ok {
		for k, v := range qualities[0] {
			fmt.Fprintf(w, "\t\t%s=%q\n", k, v)
		}
		if len(qualities[1]) > 0 {
			fmt.Fprintf(w, "\t\tnode [")
			for k, v := range qualities[1] {
				fmt.Fprintf(w, "%s=%q", k, v)
			}

			fmt.Fprintln(w, "]")
		}
	}
	for _, node := range nodes {
		fmt.Fprintf(w, "\t\t\"%s\" ", dg.nodes[node])
		node.writeNode(w)
		fmt.Fprintln(w, "")
	}
	fmt.Fprintf(w, "\t}\n")
	return nil
}

func (dg *dotGraph) Write(w io.Writer) error {
	w.Write([]byte(`
digraph g {
	rankdir = "LR"
	newrank = true
	concentrate = true
`))
	dg.clusters = make(map[string][]dotNode)
	classes := make(map[string]struct{})
	for _, node := range dg.orderedNodes {
		class := node.dotClass()
		dg.clusters[class] = append(dg.clusters[class], node)
		classes[class] = struct{}{}
	}
	for _, class := range nodeOrder {
		err := dg.WriteClass(w, class)
		if err != nil {
			return err
		}
		delete(classes, class)
	}
	if len(classes) > 0 {
		return fmt.Errorf("unordered classes: %v", classes)
	}
	for _, edge := range dg.edges {
		edge.writeEdge(w, dg)
	}
	fmt.Fprintln(w, "}")
	return nil
}

// pointerEdge is a naive edge that just represents two pointer-shaped things
type pointerEdge struct {
	from, to dotNode
	label    string
	style    string
}

func (d *pointerEdge) writeEdge(w io.Writer, dg *dotGraph) {
	var style string
	if d.style != "" {
		style = fmt.Sprintf(" style=%q", d.style)
	}
	fmt.Fprintf(w, "	%q -> %q [label=%q%s]\n", dg.nodes[d.from], dg.nodes[d.to], d.label, style)
}

func (dg *dotGraph) addEdge(from, to dotNode, extra ...string) {
	var label, style string
	switch len(extra) {
	case 2:
		style = extra[1]
		fallthrough
	case 1:
		label = extra[0]
	}
	edge := &pointerEdge{from, to, label, style}
	dg.edges = append(dg.edges, edge)
}

// RBF backend to Graphviz glue

func (r *rbfTxStore) addTo(dg *dotGraph) {
	if dg.consider(r) {
		return
	}
	for _, dbqc := range r.dbs {
		dg.enqueue(dbqc)
		dg.addEdge(r, dbqc)
	}
}

func (r *rbfDBQueryContexts) addTo(dg *dotGraph) {
	if dg.consider(r) {
		return
	}
	for tx, qc := range r.queryContexts {
		dg.enqueue(qc)
		dg.enqueue(tx)
		dg.addEdge(r, qc)
		dg.addEdge(r, tx, qc.name)
	}
}

func (r *rbfQueryContext) addTo(dg *dotGraph) {
	if dg.consider(r) {
		return
	}
	dg.addEdge(r, r.txStore, "", "dashed")
	for _, wrappers := range r.queries {
		dg.enqueue(wrappers)
		dg.addEdge(r, wrappers)
	}
}

func (r *rbfTxWrappers) addTo(dg *dotGraph) {
	if dg.consider(r) {
		return
	}
	dg.enqueue(r.db)
	dg.addEdge(r, r.db, "", "dashed")
	for _, reader := range r.queries {
		switch v := reader.(type) {
		case *rbfQueryRead:
			dg.enqueue(v)
			dg.addEdge(r, v)
		case *rbfQueryWrite:
			dg.enqueue(v)
			dg.addEdge(r, v)
		default:
			unk := &unknownEntry{v}
			dg.enqueue(unk)
			dg.addEdge(r, unk)
		}
	}
}

func (r *rbfTxStore) dotClass() string {
	return "txstore"
}

func (r *rbfTxStore) dotId() string {
	return fmt.Sprintf("txstore-%p", r)
}

func (r *rbfTxStore) writeNode(w io.Writer) {
	fmt.Fprintf(w, `[label=<rbfTxStore<BR/>[<FONT POINT-SIZE="12" FACE="courier">%s</FONT>]>]`, r.rootPath)
}

func (r *rbfDBQueryContexts) dotClass() string {
	return "rdqc"
}

func (r *rbfDBQueryContexts) dotId() string {
	return fmt.Sprintf("rdqc-%p", r)
}

func (r *rbfDBQueryContexts) writeNode(w io.Writer) {
	var locked string
	var maybeError = colorBlack
	if r.lockCheck.TryLock() {
		r.lockCheck.Unlock()
	} else {
		locked = fmt.Sprintf("<BR/><FONT COLOR=%q>[LOCKED]</FONT>", colorWrite)
	}
	// We'll want something like this when we get to adding the RBF backend.
	if false {
		if r.db == nil {
			maybeError = colorError
		}
	}
	fmt.Fprintf(w, `[label=<rbfDBQueryContexts<BR/><FONT POINT-SIZE="12" FACE="courier">%s</FONT><BR/>[DB %p]%s> color=%q]`, r.key, r.db, locked, maybeError)
}

func (r *rbfTxWrappers) dotClass() string {
	return "txw"
}

func (r *rbfTxWrappers) dotId() string {
	return fmt.Sprintf("txw-%p", r)
}

func (r *rbfTxWrappers) writeNode(w io.Writer) {
	var write string
	if r.writeTx {
		write = "write "
	}
	fmt.Fprintf(w, `[label=<rbfTxWrappers<BR/><FONT POINT-SIZE="12" FACE="courier">%s</FONT><BR/>[%sTx %p]> style="filled" fillcolor=%q]`, r.key, write, r.tx, colorWritable[r.writeTx])
}

func (r *rbfQueryContext) dotClass() string {
	return "rqc"
}

func (r *rbfQueryContext) dotId() string {
	return fmt.Sprintf("rqc-%p", r)
}

func (r *rbfQueryContext) writeNode(w io.Writer) {
	writeScope := "[read only]"
	if r.scope != nil {
		writeScope = r.scope.String()
		if len(writeScope) > 20 {
			writeScope = writeScope[:20] + "..."
		}
		writeScope = fmt.Sprintf("scope: %s", writeScope)
	}
	fmt.Fprintf(w, `[label=<rbfQueryContext<BR/>%s<BR/>%s> style="filled" fillcolor=%q]`, r.name, writeScope, colorWritable[r.scope != nil])
}

func (r *rbfQueryRead) dotClass() string {
	return "qrw"
}

func (r *rbfQueryRead) dotId() string {
	return fmt.Sprintf("qrw-%p", r)
}

func (r *rbfQueryRead) addTo(dg *dotGraph) {
	dg.consider(r)
}

func (r *rbfQueryRead) writeNode(w io.Writer) {
	fmt.Fprintf(w, `[label=<rbfQueryRead<BR/>[<FONT POINT-SIZE="12" FACE="courier">%s</FONT>]> style="filled" fillcolor=%q]`, r.fk, colorRead)
}

func (r *rbfQueryWrite) dotClass() string {
	return "qrw"
}

func (r *rbfQueryWrite) dotId() string {
	return fmt.Sprintf("qrw-%p", r)
}

func (r *rbfQueryWrite) addTo(dg *dotGraph) {
	dg.consider(r)
}

func (r *rbfQueryWrite) writeNode(w io.Writer) {
	fmt.Fprintf(w, `[label=<rbfQueryWrite<BR/>[<FONT POINT-SIZE="12" FACE="courier">%s</FONT>]> style="filled" fillcolor=%q]`, r.fk, colorWrite)
}

type unknownEntry struct {
	data interface{}
}

func (u *unknownEntry) dotClass() string {
	return "unk"
}

func (u *unknownEntry) dotId() string {
	return fmt.Sprintf("unk-%p", u)
}

func (u *unknownEntry) writeNode(w io.Writer) {
	fmt.Fprintf(w, "[label=\"unknown %T\"]", u.data)
}

func (u *unknownEntry) addTo(dg *dotGraph) {
	dg.consider(u)
}
