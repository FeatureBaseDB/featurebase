package cfg

// DefaultMaxSize is the default mmap size and therefore the maximum allowed
// size of the database. The size can be increased by updating the DB.MaxSize
// and reopening the database. This setting mainly affects virtual space usage.
const DefaultMaxSize = 256 * (1 << 20) // 256MB

// DefaultMaxWALSize is the default mmap size and therefore the maximum allowed
// size of the WAL. The size can be increased by updating the DB.MaxWALSize
// and reopening the database. This setting mainly affects virtual space usage.
const DefaultMaxWALSize = 64 * (1 << 20) // 64MB
