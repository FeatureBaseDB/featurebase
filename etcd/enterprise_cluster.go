//go:build !plg
// +build !plg

// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package etcd

// allows us to conditionally build an enterprise version of
// FB that does cluster, in contrast to the PLG version.
func AllowCluster() bool {
	return true
}
