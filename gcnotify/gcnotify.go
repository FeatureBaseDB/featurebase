// Copyright 2021 Molecula Corp. All rights reserved.
package gcnotify

import (
	"github.com/CAFxX/gcnotifier"
	"github.com/molecula/featurebase/v2"
)

// Ensure ActiveGCNotifier implements interface.
var _ pilosa.GCNotifier = &activeGCNotifier{}

type activeGCNotifier struct {
	gcn *gcnotifier.GCNotifier
}

// NewActiveGCNotifier creates an active GCNotifier.
func NewActiveGCNotifier() *activeGCNotifier {
	return &activeGCNotifier{
		gcn: gcnotifier.New(),
	}
}

// Close implements the GCNotifier interface.
func (n *activeGCNotifier) Close() {
	n.gcn.Close()
}

// AfterGC implements the GCNotifier interface.
func (n *activeGCNotifier) AfterGC() <-chan struct{} {
	return n.gcn.AfterGC()
}
