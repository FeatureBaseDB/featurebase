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

func NewAuth(ClientId, ClientSecret, AuthorizeURL, TokenURL, GroupEndpointURL string) AUTH {
	a := AUTH{
		ClientId:         ClientId,
		ClientSecret:     ClientSecret,
		AuthorizeURL:     AuthorizeURL,
		TokenURL:         TokenURL,
		GroupEndpointURL: GroupEndpointURL,
	}

	return a
}
