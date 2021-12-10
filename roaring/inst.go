//go:build btreeInstrumentation
// +build btreeInstrumentation

package roaring

type dTree struct {
	t *tree
}

type treeInst struct {
	deCopied int64
}

func (t *tree) didCopy(n int) {
	t.deCopied += int64(n)
}

func (d *d) didCopy(n int) {
	d.t.deCopied += int64(n)
}

func (t *tree) countCopies() int64 {
	return t.deCopied
}

func (d *d) setTree(t *tree) {
	d.t = t
}
