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

// SecurityManager provides the ability to limit access to restricted endpoints
// during cluster configuration.
type SecurityManager interface {
	SetRestricted()
	SetNormal()
}

// NopSecurityManager provides a no-op implementation of the SecurityManager interface.
type NopSecurityManager struct {
}

// SetRestricted no-op.
func (sdm *NopSecurityManager) SetRestricted() {}

// SetNormal no-op.
func (sdm *NopSecurityManager) SetNormal() {}
