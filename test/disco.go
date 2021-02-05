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
	"io/ioutil"
	"net"
	"strings"
	"time"

	"github.com/pilosa/pilosa/v2/etcd"
	"github.com/pilosa/pilosa/v2/server"
)

type Ports struct {
	LsnC  *net.TCPListener
	PortC int

	LsnP  *net.TCPListener
	PortP int

	Grpc int
}

func (ports *Ports) Close() error {
	err := ports.LsnC.Close()
	err2 := ports.LsnP.Close()
	if err != nil {
		return err
	}
	return err2
}

//GenPortsConfig creates specific configuration for etcd.
func GenPortsConfig(ports []Ports) []*server.Config {
	cfgs := make([]*server.Config, len(ports))
	clusterURLs := make([]string, len(ports))
	for i := range cfgs {
		name := fmt.Sprintf("server%d", i)
		clusterName := "cluster-abc123"

		lsnC, portC := ports[i].LsnC, ports[i].PortC
		lClientURL := fmt.Sprintf("http://localhost:%d", portC)
		lsnP, portP := ports[i].LsnP, ports[i].PortP
		lPeerURL := fmt.Sprintf("http://localhost:%d", portP)

		discoDir := ""
		if d, err := ioutil.TempDir("", "disco."); err == nil {
			discoDir = d
		}

		cfgs[i] = &server.Config{
			Name:     name,
			BindGRPC: fmt.Sprintf(":%d", ports[i].Grpc),
			Etcd: etcd.Options{
				Dir:           discoDir,
				LClientURL:    lClientURL,
				AClientURL:    lClientURL,
				LPeerURL:      lPeerURL,
				APeerURL:      lPeerURL,
				HeartbeatTTL:  5 * int64(time.Second),
				LPeerSocket:   []*net.TCPListener{lsnP},
				LClientSocket: []*net.TCPListener{lsnC},
			},
		}
		cfgs[i].Cluster.Name = clusterName

		clusterURLs[i] = fmt.Sprintf("%s=%s", name, lPeerURL)
	}
	for i := range cfgs {
		cfgs[i].Etcd.InitCluster = strings.Join(clusterURLs, ",")
	}

	return cfgs
}

func NewPorts(lsn []*net.TCPListener) []Ports {
	var out []Ports

	n := len(lsn)
	ports := make([]int, n)
	for i := 0; i < n; i++ {
		ports[i] = lsn[i].Addr().(*net.TCPAddr).Port
	}

	for i := 0; i < n; i = i + 3 {
		out = append(out, Ports{
			LsnC:  lsn[i],
			PortC: ports[i],
			LsnP:  lsn[i+1],
			PortP: ports[i+1],

			Grpc: ports[i+2],
		})
		// make Grpc port available to
		// be rebound.
		lsn[i+2].Close()
	}

	return out
}
