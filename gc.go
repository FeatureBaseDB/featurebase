// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

// Ensure nopGCNotifier implements interface.
var _ GCNotifier = &nopGCNotifier{}

// GCNotifier represents an interface for garbage collection notificationss.
type GCNotifier interface {
	Close()
	AfterGC() <-chan struct{}
}

func init() {
	NopGCNotifier = &nopGCNotifier{}
}

// NopGCNotifier represents a GCNotifier that doesn't do anything.
var NopGCNotifier GCNotifier

type nopGCNotifier struct{}

// Close is a no-op implementation of GCNotifier Close method.
func (n *nopGCNotifier) Close() {}

// AfterGC is a no-op implementation of GCNotifier AfterGC method.
func (c *nopGCNotifier) AfterGC() <-chan struct{} {
	return nil
}
