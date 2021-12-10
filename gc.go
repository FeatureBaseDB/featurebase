// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

// Ensure nopGCNotifier implements interface.
var _ GCNotifier = &nopGCNotifier{}

// GCNotifier represents an interface for garbage collection notificationss.
type GCNotifier interface {
	Close()
	AfterGC() <-chan struct{}
}

// NopGCNotifier represents a GCNotifier that doesn't do anything.
var NopGCNotifier GCNotifier = &nopGCNotifier{}

type nopGCNotifier struct{}

// Close is a no-op implementation of GCNotifier Close method.
func (n *nopGCNotifier) Close() {}

// AfterGC is a no-op implementation of GCNotifier AfterGC method.
func (n *nopGCNotifier) AfterGC() <-chan struct{} {
	return nil
}
