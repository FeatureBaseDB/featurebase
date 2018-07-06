// +build !386

package pilosa

// defaultMapSize is the default size of mapped memory for the translate store.
// It is passed as an int to syscall.Mmap and so can only be larger than 2^31 on
// 64bit systems.
const defaultMapSize = 10 * (1 << 30) // 10GB
