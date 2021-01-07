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
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/test/port"
)

//GenDisCoConfig creates specific configuration for etcd.
func GenDisCoConfig(clusterSize int) []*server.Config {
	cfgs := make([]*server.Config, clusterSize)

	clusterURLs := make([]string, clusterSize)
	for i := range cfgs {
		name := fmt.Sprintf("server%d", i)
		lClientURL := fmt.Sprintf("http://localhost:%d", port.GlobalPortMap.MustGetPort())
		lPeerURL := fmt.Sprintf("http://localhost:%d", port.GlobalPortMap.MustGetPort())
		cfgs[i] = &server.Config{
			BindGRPC: port.ColonZeroString(),
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
