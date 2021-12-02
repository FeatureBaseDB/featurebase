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

type AUTH struct {
	ClientId         string
	ClientSecret     string
	AuthorizeURL     string
	TokenURL         string
	GroupEndpointURL string
}

// func (c *config) Init(ClientId, ClientSecret, AuthorizeURL, TokenURL, GroupEndpointURL string) {
// 	c.ClientId = ClientId
// 	c.ClientSecret = ClientSecret
// 	c.AuthorizeURL = AuthorizeURL
// 	c.TokenURL = TokenURL
// 	c.GroupEndpointURL = GroupEndpointURL
// }

// apiOption is a functional option type for pilosa.API
type authOption func(*AUTH) error

func OptAuth(ClientId, ClientSecret, AuthorizeURL, TokenURL, GroupEndpointURL string) authOption {
	return func(a *AUTH) error {
		a.ClientId = ClientId
		a.ClientSecret = ClientSecret
		a.AuthorizeURL = AuthorizeURL
		a.TokenURL = TokenURL
		a.GroupEndpointURL = GroupEndpointURL
		return nil
	}
}

// redirectURL
