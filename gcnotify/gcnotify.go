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
