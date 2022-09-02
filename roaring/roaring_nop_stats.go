// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
//go:build !roaringstats
// +build !roaringstats

package roaring

// statsCount does nothing, because you aren't building with
// the "roaringstats" build tag.
func statsHit(string) {
}
