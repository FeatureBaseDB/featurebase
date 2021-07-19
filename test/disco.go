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
	"net"
	"strings"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/etcd"
	"github.com/molecula/featurebase/v2/server"
	"github.com/molecula/featurebase/v2/testhook"
	"github.com/pkg/errors"
)

type Ports struct {
	LsnC  *net.TCPListener
	PortC int

	LsnP  *net.TCPListener
	PortP int

	LsnG *net.TCPListener
	Grpc int
}

func (ports *Ports) Close() error {
	err := ports.LsnC.Close()
	err2 := ports.LsnP.Close()
	err3 := ports.LsnG.Close()
	if err != nil {
		return err
	}
	if err2 != nil {
		return err2
	}
	return err3
}

// listenerPortURL builds a TCP listener and corresponding http://localhost:%d
// URL, and returns those.
func listenerWithURL() (listener *net.TCPListener, url string, err error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return listener, url, err
	}
	listener = l.(*net.TCPListener)
	port := listener.Addr().(*net.TCPAddr).Port
	url = fmt.Sprintf("http://localhost:%d", port)
	return listener, url, err
}

// GetPortsGenConfigs creates listener ports, and updates the configurations
// of servers to match these created ports, including cross-references
// like updating the InitCluster values in the Etcd configs.
func GetPortsGenConfigs(tb testing.TB, nodes []*Command) error {
	peerUrls := make([]string, len(nodes))
	for i := range nodes {
		if nodes[i].Config == nil {
			nodes[i].Config = &server.Config{}
		}
		config := nodes[i].Config
		name := fmt.Sprintf("server%d", i)
		clusterName := fmt.Sprintf("cluster-%s", tb.Name())
		discoDir, err := testhook.TempDir(tb, "disco.")
		if err != nil {
			return errors.Wrap(err, "creating temp directory")
		}
		clientListener, clientURL, err := listenerWithURL()
		if err != nil {
			return errors.Wrap(err, "creating client listener")
		}
		peerListener, peerURL, err := listenerWithURL()
		if err != nil {
			return errors.Wrap(err, "creating peer listener")
		}
		grpcListener, grpcUrl, err := listenerWithURL()
		if err != nil {
			return errors.Wrap(err, "creating gRPC listener")
		}
		// for grpc, we don't want the http part...
		colon := strings.LastIndexByte(grpcUrl, ':')
		if colon != -1 {
			grpcUrl = grpcUrl[colon:]
		}
		config.Name = name
		config.Cluster.Name = clusterName
		config.BindGRPC = grpcUrl
		config.GRPCListener = grpcListener
		config.Etcd = etcd.Options{
			Dir:              discoDir,
			LClientURL:       clientURL,
			AClientURL:       clientURL,
			LPeerURL:         peerURL,
			APeerURL:         peerURL,
			HeartbeatTTL:     60,
			LPeerSocket:      []*net.TCPListener{peerListener},
			LClientSocket:    []*net.TCPListener{clientListener},
			BootstrapTimeout: 50 * time.Millisecond,
		}
		peerUrls[i] = fmt.Sprintf("%s=%s", name, peerURL)
	}
	allPeerUrls := strings.Join(peerUrls, ",")
	for i := range nodes {
		nodes[i].Config.Etcd.InitCluster = allPeerUrls
	}
	return nil
}

//GenPortsConfig creates specific configuration for etcd.
func GenPortsConfig(tb testing.TB, ports []Ports) []*server.Config {
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
		if d, err := testhook.TempDir(tb, "disco."); err == nil {
			discoDir = d
		}

		cfgs[i] = &server.Config{
			Name:         name,
			BindGRPC:     fmt.Sprintf(":%d", ports[i].Grpc),
			GRPCListener: ports[i].LsnG,
			Etcd: etcd.Options{
				Dir:           discoDir,
				LClientURL:    lClientURL,
				AClientURL:    lClientURL,
				LPeerURL:      lPeerURL,
				APeerURL:      lPeerURL,
				HeartbeatTTL:  5,
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
			Grpc:  ports[i+2],
			LsnG:  lsn[i+2],
		})
	}

	return out
}
