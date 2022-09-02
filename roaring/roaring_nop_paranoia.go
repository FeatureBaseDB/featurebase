// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
//go:build !roaringparanoia
// +build !roaringparanoia

package roaring

const roaringParanoia = false

// CheckN verifies that a container's cached count is correct, but
// there are two versions; this is the one which doesn't actually
// do anything, because the check is expensive. Which one you get is
// controlled by the roaringparanoia build tag.
func (c *Container) CheckN() {
}
