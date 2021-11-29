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

package server

type Options struct {
	Enable              bool   `toml:"enable"`
	IdentityProviderURL string `toml:"identity-provider-url"`
	AuthorizeURL        string `toml:"authorize-url"`
	UserInfoURL         string `toml:"user-info-url"`
	ClientId            string `toml:"client-id"`
}

func authenticateUser(opt Options) (resp bool) {

	// fmt.Println("IdentityProviderURL", opt.IdentityProviderURL)
	// fmt.Println("AuthorizeURL", opt.AuthorizeURL)
	// fmt.Println("UserInfoURL", opt.UserInfoURL)
	// fmt.Println("ClientId", opt.ClientId)

	resp = false

	return resp
}
