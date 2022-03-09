// Copyright 2021 Molecula Corp. All rights reserved.
//go:build !386
// +build !386

package cfg

// DefaultMaxSize is the default mmap size and therefore the maximum allowed
// size of the database. The size can be increased by updating the DB.MaxSize
// and reopening the database. This setting mainly affects virtual space usage.
const DefaultMaxSize = 4 * (1 << 30)

// DefaultMaxWALSize is the default mmap size and therefore the maximum allowed
// size of the WAL. The size can be increased by updating the DB.MaxWALSize
// and reopening the database. This setting mainly affects virtual space usage.
const DefaultMaxWALSize = 4 * (1 << 30)

// DefaultMaxDelete is the maximum number of bits that will be deleted in a single batch
const DefaultMaxDelete = 65536
