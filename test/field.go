// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package test

import pilosa "github.com/featurebasedb/featurebase/v3"

// Field represents a test wrapper for pilosa.Field.
type Field struct {
	*pilosa.Field
}
