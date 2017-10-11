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
	flags.StringVarP(&srv.Config.DataDir, "data-dir", "d", srv.Config.DataDir, "Directory to store pilosa data files.")
	flags.StringVarP(&srv.Config.Bind, "bind", "b", srv.Config.Bind, "Default URI on which pilosa should listen.")
	flags.StringVarP(&srv.Config.GossipPort, "gossip-port", "", srv.Config.GossipPort, "Port to which pilosa should bind for internal state sharing.")
	flags.StringVarP(&srv.Config.GossipSeed, "gossip-seed", "", srv.Config.GossipSeed, "Host with which to seed the gossip membership.")
	flags.IntVarP(&srv.Config.MaxWritesPerRequest, "max-writes-per-request", "", srv.Config.MaxWritesPerRequest, "Number of write commands per request.")
	flags.IntVarP(&srv.Config.Cluster.ReplicaN, "cluster.replicas", "", srv.Config.Cluster.ReplicaN, "Number of hosts each piece of data should be stored on.")
	flags.StringSliceVarP(&srv.Config.Cluster.Hosts, "cluster.hosts", "", srv.Config.Cluster.Hosts, "Comma separated list of hosts in cluster.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Cluster.PollInterval), "cluster.poll-interval", "", (time.Duration)(srv.Config.Cluster.PollInterval), "Polling interval for cluster.") // TODO what actually is this?
	flags.DurationVarP((*time.Duration)(&srv.Config.Cluster.LongQueryTime), "cluster.long-query-time", "", (time.Duration)(srv.Config.Cluster.LongQueryTime), "Long Query Time.")
	flags.StringVarP(&srv.Config.Plugins.Path, "plugins.path", "", srv.Config.Plugins.Path, "Path to plugin directory.")
	flags.StringVar(&srv.Config.LogPath, "log-path", srv.Config.LogPath, "Log path")
	flags.DurationVarP((*time.Duration)(&srv.Config.AntiEntropy.Interval), "anti-entropy.interval", "", (time.Duration)(srv.Config.AntiEntropy.Interval), "Interval at which to run anti-entropy routine.")
	flags.StringVarP(&srv.CPUProfile, "profile.cpu", "", srv.CPUProfile, "Where to store CPU profile.")
	flags.DurationVarP(&srv.CPUTime, "profile.cpu-time", "", srv.CPUTime, "CPU profile duration.")
	flags.StringVarP(&srv.Config.Cluster.Type, "cluster.type", "", srv.Config.Cluster.Type, "Determine how the cluster handles membership and state sharing. Choose from [static, gossip]")
	flags.StringVarP(&srv.Config.Metric.Service, "metric.service", "", srv.Config.Metric.Service, "Default URI on which pilosa should listen.")
	flags.StringVarP(&srv.Config.Metric.Host, "metric.host", "", srv.Config.Metric.Host, "Default URI to send metrics.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Metric.PollInterval), "metric.poll-interval", "", (time.Duration)(srv.Config.Metric.PollInterval), "Polling interval metrics.")
}
