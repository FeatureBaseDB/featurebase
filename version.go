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

var Enterprise = "0"
var EnterpriseEnabled = false
var Version = "v0.0.0"
var BuildTime = "not recorded"

// init sets the EnterpriseEnabled bool, based on the Enterprise string.
// This is needed because bools cannot be set with ldflags.
func init() { // nolint: gochecknoinits
	if Enterprise == "1" {
		EnterpriseEnabled = true
	}
}
