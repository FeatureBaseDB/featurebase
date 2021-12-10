package test

import (
	"github.com/molecula/featurebase/v2"
)

// Field represents a test wrapper for pilosa.Field.
type Field struct {
	*pilosa.Field
}
