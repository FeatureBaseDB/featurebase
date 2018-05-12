// +build enterprise

package roaring

import "github.com/pilosa/pilosa/enterprise/b"

func NewContainers() *b.BTreeContainers {
	return &b.BTreeContainers{}
}
