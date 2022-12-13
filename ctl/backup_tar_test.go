package ctl

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/test"
)

func TestBackupTarCommand_Run(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)
	indexName := "backuptar"

	// this might produce some annoying spam in tests but we need to make sure log messages to
	// stdout are being redirected properly when the tarfile is also going to stdout
	cm := NewBackupTarCommand(os.Stdout)
	hostport := cmd.API.Node().URI.HostPort()
	cm.Host = hostport
	dir := t.TempDir()
	cm.OutputPath = filepath.Join(dir, "backuptest.tar")

	_, err := cmd.API.CreateIndex(context.Background(), indexName, pilosa.IndexOptions{Keys: true, TrackExistence: true})
	if err != nil {
		t.Fatalf("creating test index: %v", err)
	}
	_, err = cmd.API.CreateField(context.Background(), indexName, "f", pilosa.OptFieldKeys())
	if err != nil {
		t.Fatalf("creating test field: %v", err)
	}

	cm.Index = indexName
	if err := cm.Run(context.Background()); err != nil {
		t.Fatalf("BackupTarCommand Run error: %s", err)
	}

	oldpath := cm.OutputPath
	cm.OutputPath = "-"
	cfpath := filepath.Join(dir, "stdouttest.tar") //capture file
	cf, err := os.Create(cfpath)
	if err != nil {
		t.Fatalf("opening file to compare file and stdout outputs: %v", err)
	}
	defer cf.Close()
	// I don't like this at all but it's all i'm really finding for capturing os.Stdout
	old := os.Stdout
	defer func() { os.Stdout = old }()
	os.Stdout = cf
	if err := cm.Run(context.Background()); err != nil {
		t.Fatalf("BackupTarCommand Run error: %s", err)
	}
	fdata, err := os.ReadFile(oldpath)
	if err != nil {
		t.Fatalf("unable to read from direct-to-file backup: %v", err)
	}
	cdata, err := os.ReadFile(cfpath)
	if err != nil {
		t.Fatalf("unable to read from captured stdout backup: %v", err)
	}
	if !bytes.Equal(fdata, cdata) {
		t.Fatalf("backing up to file and to stdout produced different results")
	}
}
