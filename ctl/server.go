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

package ctl

import (
	"time"

	"github.com/pilosa/pilosa/server"
	"github.com/spf13/cobra"
)

// BuildServerFlags attaches a set of flags to the command for a server instance.
func BuildServerFlags(cmd *cobra.Command, srv *server.Command) {
	flags := cmd.Flags()
	flags.StringVarP(&srv.Config.DataDir, "data-dir", "d", "~/.pilosa", "Directory to store pilosa data files.")
	flags.StringVarP(&srv.Config.Host, "bind", "b", ":10101", "Default URI on which pilosa should listen.")
	flags.IntVarP(&srv.Config.MaxWritesPerRequest, "max-writes-per-request", "", srv.Config.MaxWritesPerRequest, "Number of write commands per request.")
	flags.IntVarP(&srv.Config.Cluster.ReplicaN, "cluster.replicas", "", 1, "Number of hosts each piece of data should be stored on.")
	flags.StringSliceVarP(&srv.Config.Cluster.Hosts, "cluster.hosts", "", []string{}, "Comma separated list of hosts in cluster.")
	flags.StringSliceVarP(&srv.Config.Cluster.InternalHosts, "cluster.internal-hosts", "", []string{}, "Comma separated list of hosts in cluster used for internal communication.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Cluster.PollingInterval), "cluster.poll-interval", "", time.Minute, "Polling interval for cluster.") // TODO what actually is this?
	flags.StringVarP(&srv.Config.Plugins.Path, "plugins.path", "", "", "Path to plugin directory.")
	flags.StringVar(&srv.Config.LogPath, "log-path", "", "Log path")
	flags.DurationVarP((*time.Duration)(&srv.Config.AntiEntropy.Interval), "anti-entropy.interval", "", time.Minute*10, "Interval at which to run anti-entropy routine.")
	flags.StringVarP(&srv.CPUProfile, "profile.cpu", "", "", "Where to store CPU profile.")
	flags.DurationVarP(&srv.CPUTime, "profile.cpu-time", "", 30*time.Second, "CPU profile duration.")
	flags.StringVarP(&srv.Config.Cluster.Type, "cluster.type", "", "static", "Determine how the cluster handles membership and state sharing. Choose from [static, http, gossip]")
	flags.StringVarP(&srv.Config.Cluster.GossipSeed, "cluster.gossip-seed", "", "", "Host with which to seed the gossip membership.")
	flags.StringVarP(&srv.Config.Cluster.InternalPort, "cluster.internal-port", "", "", "Port to which pilosa should bind for internal state sharing.")
}
