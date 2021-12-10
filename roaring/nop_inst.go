//go:build !btreeInstrumentation
// +build !btreeInstrumentation

package roaring

//lint:ignore U1000 this is conditional on a build flag
//nolint:unused
type dTree struct {
}

//lint:ignore U1000 this is conditional on a build flag
//nolint:unused
type treeInst struct {
}

func (t *tree) didCopy(n int) {
}

func (d *d) didCopy(n int) {
}

func (t *tree) countCopies() int64 {
	return 0
}

func (d *d) setTree(t *tree) {
}
