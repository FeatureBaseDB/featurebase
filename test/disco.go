// Copyright 2021 Molecula Corp. All rights reserved.
package test

import (
	"fmt"
	"net"
	"strings"
	"testing"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/etcd"
	"github.com/molecula/featurebase/v3/server"
	"github.com/molecula/featurebase/v3/testhook"
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
		clientURL := pilosa.EtcdUnixSocket(tb)
		peerURL := pilosa.EtcdUnixSocket(tb)
		config.Etcd = etcd.Options{
			Dir:           discoDir,
			LClientURL:    clientURL,
			AClientURL:    clientURL,
			LPeerURL:      peerURL,
			APeerURL:      peerURL,
			HeartbeatTTL:  60,
			UnsafeNoFsync: true,
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

		lClientURL := pilosa.EtcdUnixSocket(tb)
		lPeerURL := pilosa.EtcdUnixSocket(tb)

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
				UnsafeNoFsync: true,
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
