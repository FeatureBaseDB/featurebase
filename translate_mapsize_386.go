package pilosa

// defaultMapSize is the default size of mapped memory for the translate store.
// It is passed as an int to syscall.Mmap and so must be < 2^31
const defaultMapSize = (1 << 31) - 1 // 2GB
