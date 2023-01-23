// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"io"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentracer"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/server"
	"github.com/featurebasedb/featurebase/v3/tracing"
	"github.com/featurebasedb/featurebase/v3/tracing/opentracing"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	jaegercfg "github.com/uber/jaeger-client-go/config"

	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

// Server is global so that tests can control and verify it.
var Server *server.Command
var holder *server.Command

// newHolderCmd creates a FeatureBase server for just long enough to open the
// holder, then shuts it down again.
func newHolderCmd(stderr io.Writer) *cobra.Command {
	holder = server.NewCommand(stderr)
	serveCmd := &cobra.Command{
		Use:   "holder",
		Short: "Load FeatureBase.",
		Long: `featurebase holder starts (and immediately stops) FeatureBase.

It opens the data directory and loads it, then shuts down immediately.
This is only useful for diagnostic use.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Start & run the server.
			if err := holder.UpAndDown(); err != nil {
				return errors.Wrap(err, "running server")
			}
			return nil
		},
	}

	// Attach flags to the command.
	ctl.BuildServerFlags(serveCmd, holder)
	return serveCmd
}

// newServeCmd creates a FeatureBase server and runs it with command line flags.
func newServeCmd(stderr io.Writer) *cobra.Command {
	Server = server.NewCommand(stderr)
	serveCmd := &cobra.Command{
		Use:   "server",
		Short: "Run FeatureBase.",
		Long: `featurebase server runs FeatureBase.

It will load existing data from the configured
directory and start listening for client connections
on the configured port.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Start & run the server.
			if err := Server.Start(); err != nil {
				return considerUsageError(cmd, errors.Wrap(err, "running server"))
			}
			// anything past here is definitely not a usage error
			cmd.SilenceErrors = true
			cmd.SilenceUsage = true
			if Server.Config.DataDog.Enable {
				opts := make([]profiler.ProfileType, 0)
				if Server.Config.DataDog.CPUProfile {
					opts = append(opts, profiler.CPUProfile)
				}
				if Server.Config.DataDog.HeapProfile {
					opts = append(opts, profiler.HeapProfile)
				}
				if Server.Config.DataDog.BlockProfile {
					opts = append(opts, profiler.BlockProfile)
				}
				if Server.Config.DataDog.GoroutineProfile {
					opts = append(opts, profiler.GoroutineProfile)
				}
				if Server.Config.DataDog.MutexProfile {
					opts = append(opts, profiler.MutexProfile)
				}
				err := profiler.Start(
					profiler.WithService(Server.Config.DataDog.Service),
					profiler.WithEnv(Server.Config.DataDog.Env),
					profiler.WithVersion(Server.Config.DataDog.Version),
					profiler.WithTags(Server.Config.DataDog.Tags),
					profiler.WithProfileTypes(
						opts...,
					),
				)
				if err != nil {
					return errors.Wrap(err, "starting datadog")
				}
				defer profiler.Stop()
			}

			if Server.Config.Tracing.SamplerType != "off" {
				// Initialize tracing in the command since it is global.
				var cfg jaegercfg.Configuration
				cfg.ServiceName = "pilosa"
				cfg.Sampler = &jaegercfg.SamplerConfig{
					Type:  Server.Config.Tracing.SamplerType,
					Param: Server.Config.Tracing.SamplerParam,
				}
				cfg.Reporter = &jaegercfg.ReporterConfig{
					LocalAgentHostPort: Server.Config.Tracing.AgentHostPort,
				}
				tracer, closer, err := cfg.NewTracer()
				if err != nil {
					return errors.Wrap(err, "initializing jaeger tracer")
				}
				defer closer.Close()
				tracing.GlobalTracer = opentracing.NewTracer(tracer, Server.Logger())

			} else if Server.Config.DataDog.EnableTracing { // Give preference to legacy support of jaeger
				t := opentracer.New(tracer.WithServiceName(Server.Config.DataDog.Service))
				defer tracer.Stop()
				tracing.GlobalTracer = opentracing.NewTracer(t, Server.Logger())
			}
			return errors.Wrap(Server.Wait(), "waiting on Server")
		},
	}

	// Attach flags to the command.
	ctl.BuildServerFlags(serveCmd, Server)
	return serveCmd
}
