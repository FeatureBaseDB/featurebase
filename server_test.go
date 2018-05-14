package pilosa_test

import (
	"context"
	"testing"
	"time"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

// TestMonitorAntiEntropy is a regression test which which caught a bug where
// pilosa.Server was not having its remoteClient field set by an option and so
// it was using a nil client in monitorAntiEntropy.
func TestMonitorAntiEntropy(t *testing.T) {
	cluster := test.MustRunMainWithCluster(t, 3, test.OptAntiEntropyInterval(time.Millisecond*1))
	client := cluster[1].Client()
	err := client.EnsureIndex(context.Background(), "balh", pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	err = client.EnsureFrame(context.Background(), "balh", "fralh", pilosa.FrameOptions{})
	if err != nil {
		t.Fatalf("creating frame: %v", err)
	}

	time.Sleep(time.Millisecond * 2)
	for _, m := range cluster {
		err := m.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

}
