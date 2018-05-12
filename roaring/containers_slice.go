// +build !enterprise

package roaring

func NewContainers() *SliceContainers {
	return &SliceContainers{}
}
