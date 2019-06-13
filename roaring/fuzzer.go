// +build gofuzz

package roaring

func FuzzBitmapUnmarshalBinary(data []byte) int {
	b := NewBitmap()
	err := b.UnmarshalBinary(data)
	if err != nil {
		return 0
	}
	return 1
}
