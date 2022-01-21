// Copyright 2021 Molecula Corp. All rights reserved.
package test

import (
	"github.com/molecula/featurebase/v3"
)

// Field represents a test wrapper for pilosa.Field.
type Field struct {
	*pilosa.Field
}
