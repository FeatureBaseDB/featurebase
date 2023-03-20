package boltdb

import (
	"encoding/binary"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
)

var (
	bucketDirective     = Bucket("nodeDirective")
	keyDirectiveVersion = []byte("directiveVersion")
)

// DirectiveBuckets defines the buckets used by this package. It can be called
// during setup to create the buckets ahead of time.
var DirectiveBuckets []Bucket = []Bucket{
	bucketDirective,
}

// Ensure type implements interface.
var _ dax.DirectiveVersion = (*DirectiveVersion)(nil)

type DirectiveVersion struct {
	db *DB
}

func NewDirectiveVersion(db *DB) *DirectiveVersion {
	return &DirectiveVersion{
		db: db,
	}
}

func (d *DirectiveVersion) Increment(tx dax.Transaction, delta uint64) (uint64, error) {
	txx, ok := tx.(*Tx)
	if !ok {
		return 0, dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketDirective)
	if bkt == nil {
		return 0, errors.Errorf(ErrFmtBucketNotFound, bucketDirective)
	}

	var nextVersion uint64 = 1 // Start at 1; 0 is an invalid version.

	b := bkt.Get(keyDirectiveVersion)
	if b != nil {
		nextVersion = binary.LittleEndian.Uint64(b) + delta
	}

	vsn := make([]byte, 8)
	binary.LittleEndian.PutUint64(vsn, nextVersion)

	if err := bkt.Put(keyDirectiveVersion, vsn); err != nil {
		return 0, errors.Wrap(err, "putting next directive version")
	}

	return nextVersion, nil
}
