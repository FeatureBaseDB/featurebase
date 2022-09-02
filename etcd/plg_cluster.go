//go:build plg
// +build plg

// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package etcd

// allows us to conditionally build a version of FB that does not
// cluster, to make a demo available for our Product Led Growth.
func AllowCluster() bool {
	return false
}
