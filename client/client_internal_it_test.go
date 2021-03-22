//+build integration

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

package client

import (
	"reflect"
	"testing"

	pnet "github.com/pilosa/pilosa/v2/net"
)

func TestNewClientFromAddresses(t *testing.T) {
	cases := []struct {
		Name          string
		Hosts         []string
		ExpectErr     bool
		ExpectedHosts []pnet.URI
	}{
		{
			Name:  "Cluster",
			Hosts: []string{":10101", "node0.pilosa.com:10101", "node2.pilosa.com"},
			ExpectedHosts: []pnet.URI{
				{Scheme: "http", Port: 10101, Host: "localhost"},
				{Scheme: "http", Port: 10101, Host: "node0.pilosa.com"},
				{Scheme: "http", Port: 10101, Host: "node2.pilosa.com"},
			},
		},
		{
			Name:      "URIParseError",
			Hosts:     []string{"://"},
			ExpectErr: true,
		},
		{
			Name:          "Empty",
			Hosts:         []string{},
			ExpectedHosts: []pnet.URI{},
		},
		{
			Name:          "nil",
			ExpectedHosts: []pnet.URI{},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			cli, err := NewClient(c.Hosts)
			if c.ExpectErr {
				if err == nil {
					t.Fatalf("Did not get expected error when creating client: %v", cli.cluster.Hosts())
				}
			} else {
				if err != nil {
					t.Fatalf("Creating client from addresses: %v", err)
				}
				if actualHosts := cli.cluster.Hosts(); !reflect.DeepEqual(actualHosts, c.ExpectedHosts) {
					t.Fatalf("Unexpected hosts in client's cluster, got: %v, expected: %v", actualHosts, c.ExpectedHosts)
				}
			}
		})
	}
}

func TestDetectClusterChanges(t *testing.T) {
	c := getClient()
	defer c.Close()
	c.shardNodes.data["blah"] = make(map[uint64][]*pnet.URI)
	c.shardNodes.data["blah"][1] = []*pnet.URI{{Scheme: "zzz"}}

	c.detectClusterChanges()
}
