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

package auth

type Auth struct {
	// Enable AuthZ/AuthN for featurebase server
	Enable bool `toml:"enable"`

	// Application/Client ID
	ClientId string `toml:"client-id"`

	// Client Secret
	ClientSecret string `toml:"client-secret"`

	// Authorize URL
	AuthorizeURL string `toml:"authorize-url"`

	// Token URL
	TokenURL string `toml:"token-url"`

	// Group Endpoint URL
	GroupEndpointURL string `toml:"group-endpoint-url"`

	// Scope URL
	ScopeURL string `toml:"scope-url"`
}
