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

import (
	"io"
	"time"
)

// Diagnostics represents an interface for sending diagnostics to a consumer.
type Diagnostics interface {
	SetVersion(string)
	SetInterval(time.Duration)
	Flush() error
	Open()
	CheckVersion() error
	Set(string, interface{})
	SetLogger(io.Writer)
	EnrichWithOSInfo()
	EnrichWithMemoryInfo()
}

func init() {
	NopDiagnostics = &nopDiagnostics{}
}

// NopDiagnostics represents a Diagnostics that doesn't do anything.
var NopDiagnostics Diagnostics

type nopDiagnostics struct{}

// SetVersion is a no-op implemenetation of Diagnostics SetVersion method.
func (n *nopDiagnostics) SetVersion(v string) {}

// SetInterval is a no-op implemenetation of Diagnostics SetInterval method.
func (n *nopDiagnostics) SetInterval(i time.Duration) {}

// Flush is a no-op implemenetation of Diagnostics Flush method.
func (n *nopDiagnostics) Flush() error {
	return nil
}

// Open is a no-op implemenetation of Diagnostics Open method.
func (n *nopDiagnostics) Open() {}

// CheckVersion is a no-op implemenetation of Diagnostics CheckVersion method.
func (n *nopDiagnostics) CheckVersion() error {
	return nil
}

// Set is a no-op implemenetation of Diagnostics Set method.
func (n *nopDiagnostics) Set(k string, v interface{}) {}

// SetLogger is a no-op implemenetation of Diagnostics SetLogger method.
func (n *nopDiagnostics) SetLogger(w io.Writer) {}

// EnrichWithOSInfo is a no-op implemenetation of Diagnostics EnrichWithOSInfo method.
func (n *nopDiagnostics) EnrichWithOSInfo() {}

// EnrichWithMemoryInfo is a no-op implemenetation of Diagnostics EnrichWithMemoryInfo method.
func (n *nopDiagnostics) EnrichWithMemoryInfo() {}
