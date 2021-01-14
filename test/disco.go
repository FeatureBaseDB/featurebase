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
	"fmt"
	"strings"

	"github.com/pilosa/pilosa/v2/etcd"
	"github.com/pilosa/pilosa/v2/gossip"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/test/port"
)

type Ports struct {
	Client, Peer int
	Grpc, Gossip int //TODO remove
}

//GenPortsConfig creates specific configuration for etcd.
func GenPortsConfig(ports []Ports) []*server.Config {
	cfgs := make([]*server.Config, len(ports))
	clusterURLs := make([]string, len(ports))
	for i := range cfgs {
		name := fmt.Sprintf("server%d", i)

		var lClientURL, lPeerURL string
		lClientURL = fmt.Sprintf("http://localhost:%d", ports[i].Client)
		lPeerURL = fmt.Sprintf("http://localhost:%d", ports[i].Peer)

		cfgs[i] = &server.Config{
			Gossip: gossip.Config{
				Port: fmt.Sprint(ports[i].Gossip),
			},
			BindGRPC: port.ColonZeroString(ports[i].Grpc),
			DisCo: etcd.Options{
				Name:        name,
				Dir:         "",
				ClusterName: "bartholemuuuuu",
				LClientURL:  lClientURL,
				AClientURL:  lClientURL,
				LPeerURL:    lPeerURL,
				APeerURL:    lPeerURL,
			},
		}

		clusterURLs[i] = fmt.Sprintf("%s=%s", name, lPeerURL)
		fmt.Printf("\ndebug test/disco.go: on i=%v, GenDisCoConfig BindGRPC: %v\n", i, cfgs[i].BindGRPC)
	}
	for i := range cfgs {
		cfgs[i].DisCo.InitCluster = strings.Join(clusterURLs, ",")
	}

	return cfgs
}

func NewPorts(ports []int) []Ports {
	var out []Ports
	for i := 0; i < len(ports); i = i + 4 {
		out = append(out, Ports{
			Client: ports[i],
			Peer:   ports[i+1],
			Grpc:   ports[i+2],
			Gossip: ports[i+3],
		})
	}

	return out
}
