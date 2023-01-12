// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"io"
	"testing"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/server"
	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/featurebasedb/featurebase/v3/testhook"
)

func TestDataframeCsvLoaderCommand(t *testing.T) {
	cluster := test.MustRunCluster(t, 1, []server.CommandOption{server.OptCommandServerOptions(pilosa.OptServerIsDataframeEnabled(true))})
	defer cluster.Close()
	cmd := cluster.GetNode(0)
	t.Run("basic", func(t *testing.T) {
		cmLog := logger.NewStandardLogger(io.Discard)
		cm := NewDataframeCsvLoaderCommand(cmLog)
		file, err := testhook.TempFile(t, "import.csv")
		if err != nil {
			t.Fatalf("creating tempfile: %v", err)
		}
		_, err = file.Write([]byte("id,val__I\n1,2\n3,4\n5,6"))
		if err != nil {
			t.Fatalf("writing to tempfile: %v", err)
		}
		ctx := context.Background()
		if err != nil {
			t.Fatal(err)
		}
		index := "non-keyed"
		cmd.API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: false})

		cm.Host = cmd.API.Node().URI.HostPort()
		cm.Path = file.Name()
		cm.Index = index

		err = cm.Run(ctx)
		if err != nil {
			t.Fatalf("DataframeCsvLoader Run doesn't work: %s", err)
		}
	})
	t.Run("keyed", func(t *testing.T) {
		cmLog := logger.NewStandardLogger(io.Discard)
		cm := NewDataframeCsvLoaderCommand(cmLog)
		file, err := testhook.TempFile(t, "import_key.csv")
		if err != nil {
			t.Fatalf("creating tempfile: %v", err)
		}
		_, err = file.Write([]byte("id,val__I\nA,2\nB,4\nC,6"))
		if err != nil {
			t.Fatalf("writing to tempfile: %v", err)
		}
		ctx := context.Background()
		if err != nil {
			t.Fatal(err)
		}
		index := "keyed"
		cmd.API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: true})

		cm.Host = cmd.API.Node().URI.HostPort()
		cm.Path = file.Name()
		cm.Index = index

		err = cm.Run(ctx)
		if err != nil {
			t.Fatalf("DataframeCsvLoader Run doesn't work: %s", err)
		}
	})
}
