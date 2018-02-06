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

package test

import (
	"net/http"

	"github.com/pilosa/pilosa"
)

// Client represents a test wrapper for pilosa.Client.
type Client struct {
	*pilosa.InternalHTTPClient
}

// MustNewClient returns a new instance of Client. Panic on error.
func MustNewClient(host string, h *http.Client) *Client {
	c, err := pilosa.NewInternalHTTPClient(host, h)
	if err != nil {
		panic(err)
	}
	return &Client{InternalHTTPClient: c}
}
