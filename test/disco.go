// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package test

import (
	"fmt"
	"net"
	"strings"

	"github.com/featurebasedb/featurebase/v3/etcd"
	"github.com/featurebasedb/featurebase/v3/server"
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

// listenerWithURL builds a TCP listener and corresponding http://localhost:%d
// URL, and returns those.
func listenerWithURL() (listener *net.TCPListener, url string, err error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return listener, url, err
	}
	listener = l.(*net.TCPListener)
	port := listener.Addr().(*net.TCPAddr).Port
	url = fmt.Sprintf("http://localhost:%d", port)
	return listener, url, err
}

// GetPortsGenConfigs generates etcd configs for a number
// of nodes, including cross-references so that every node gets
// an initial cluster list pointing it at the other nodes,
// and modifies the configs of the provided Command objects
// to point to these etcd configs. It uses etcd.GenEtcdConfigs,
// which in turn creates temporary directories and the like.
func GetPortsGenConfigs(tb DirCleaner, nodes []*Command) error {
	clusterName, cfgs := etcd.GenEtcdConfigs(tb, len(nodes))
	for i := range nodes {
		if nodes[i].Config == nil {
			nodes[i].Config = &server.Config{}
		}
		config := nodes[i].Config
		grpcListener, grpcUrl, err := listenerWithURL()
		if err != nil {
			return errors.Wrap(err, "creating gRPC listener")
		}
		// for grpc, we don't want the http part...
		colon := strings.LastIndexByte(grpcUrl, ':')
		if colon != -1 {
			grpcUrl = grpcUrl[colon:]
		}
		config.Name = cfgs[i].Name
		config.Cluster.Name = clusterName
		config.BindGRPC = grpcUrl
		config.GRPCListener = grpcListener
		config.Etcd = cfgs[i]
	}
	return nil
}
