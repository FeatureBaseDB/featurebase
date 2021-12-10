// Copyright 2021 Molecula Corp. All rights reserved.
//go:build roaringparanoia
// +build roaringparanoia

package roaring

import "fmt"

const roaringParanoia = true

// CheckN verifies that the container's cached count is correct. Note
// that this has two definitions, depending on the presence of the
// roaringparanoia build tag.
func (c *Container) CheckN() {
	if c == nil {
		return
	}
	count := c.count()
	if count != c.n {
		panic(fmt.Sprintf("CheckN (%p): n %d, count %d", c, c.n, count))
	}
}
