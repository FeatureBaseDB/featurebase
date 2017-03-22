package ctl_test

import (
	"testing"

	"github.com/pilosa/pilosa"
	. "github.com/pilosa/pilosa/ctl"
	"github.com/pilosa/pilosa/internal"
)

func TestCreateMigrationBuildPlan(t *testing.T) {

	srcCluster := pilosa.Config{
		DataDir: "src-path",
		Host:    "ignored:123",
		Cluster: struct {
			ReplicaN        int                 `toml:"replicas"`
			MessengerType   string              `toml:"messenger-type"`
			Nodes           []string            `toml:"hosts"`
			PollingInterval pilosa.Duration     `toml:"polling-interval"`
			Gossip          pilosa.ConfigGossip `toml:"gossip"`
		}{ReplicaN: 2,
			MessengerType:   "N/A",
			Nodes:           []string{"a:123", "b.123", "c:123", "d:123"},
			PollingInterval: 0},
		Plugins: struct {
			Path string `toml:"path"`
		}{Path: ""},
		AntiEntropy: struct {
			Interval pilosa.Duration `toml:"interval"`
		}{Interval: 0},
	}

	destCluster := pilosa.Config{
		DataDir: "dest-path",
		Host:    "ignored:123",
		Cluster: struct {
			ReplicaN        int                 `toml:"replicas"`
			MessengerType   string              `toml:"messenger-type"`
			Nodes           []string            `toml:"hosts"`
			PollingInterval pilosa.Duration     `toml:"polling-interval"`
			Gossip          pilosa.ConfigGossip `toml:"gossip"`
		}{ReplicaN: 2,
			MessengerType:   "N/A",
			Nodes:           []string{"z:123", "y.123", "x:123", "w:123"},
			PollingInterval: 0},
		Plugins: struct {
			Path string `toml:"path"`
		}{Path: ""},
		AntiEntropy: struct {
			Interval pilosa.Duration `toml:"interval"`
		}{Interval: 0},
	}

	cluster_a := srcCluster.PilosaCluster()
	cluster_b := destCluster.PilosaCluster()
	dbs := []string{"db1", "db2", "db3"}
	sampleSchema := make([]*internal.DB, len(dbs), len(dbs))
	for i, db := range dbs {
		sampleSchema[i] = createDB(db, "profile", srcCluster.DataDir, 0, uint64(len(dbs)))
	}

	//test  same config as source and dest  produce no actions
	plan := NewMigrationPlan(srcCluster.DataDir, destCluster.DataDir)
	BuildPlan(sampleSchema, cluster_a, cluster_a, plan)
	if len(plan.Actions) != 0 {
		t.Error("No action should be added with identical configurations")
	}

	//test  should produce actions
	plan = NewMigrationPlan(srcCluster.DataDir, destCluster.DataDir)
	BuildPlan(sampleSchema, cluster_a, cluster_b, plan)
	if len(plan.Actions) == 0 {
		t.Error("there should be actions")
	}

}
func newFrameIgnore(name, rowlabel string, timeQuantum uint64, cachesize int) *internal.Frame {
	return &internal.Frame{
		Name: name,
		Meta: &internal.FrameMeta{
			TimeQuantum: string(timeQuantum),
			RowLabel:    rowlabel,
			CacheSize:   int64(cachesize),
		},
	}
}
func createDB(dbname, columnLabel, path string, timeQuantum, maxSlice uint64) *internal.DB {
	frames := []*internal.Frame{
		newFrameIgnore("a", columnLabel, timeQuantum, 0),
		newFrameIgnore("b", columnLabel, timeQuantum, 0),
		newFrameIgnore("c", columnLabel, timeQuantum, 0),
	}

	db := &internal.DB{
		Name: dbname,
		Meta: &internal.DBMeta{
			ColumnLabel: columnLabel,
			TimeQuantum: string(timeQuantum),
		},
		MaxSlice: maxSlice,
		Frames:   frames,
	}
	return db
}
