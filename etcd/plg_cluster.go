//go:build plg
// +build plg

// Copyright 2022 Molecula Corp. All rights reserved.
package etcd

// allows us to conditionally build a version of FB that does not
// cluster, to make a demo available for our Product Led Growth.
func AllowCluster() bool {
	return false
}
