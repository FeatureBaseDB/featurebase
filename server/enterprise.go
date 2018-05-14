// +build enterprise

package server

import (
	"github.com/pilosa/pilosa/enterprise/b"
	"github.com/pilosa/pilosa/roaring"
)

func init() {
	// Replace Bitmap constructor with B+Tree implementation
	roaring.NewFileBitmap = b.NewBTreeBitmap
}
