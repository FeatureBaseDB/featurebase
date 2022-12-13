package storage

import (
	"bytes"
	"io"
	"os"

	"testing"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/snapshotter"
	"github.com/molecula/featurebase/v3/dax/writelogger"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/stretchr/testify/assert"
)

func TestManagerManager(t *testing.T) {
	sdd, err := os.MkdirTemp("", "snaptest*")
	assert.NoError(t, err)
	wdd, err := os.MkdirTemp("", "wltest*")
	assert.NoError(t, err)
	defer func() {
		os.RemoveAll(sdd)
		os.RemoveAll(wdd)
	}()

	sn := snapshotter.New(snapshotter.Config{
		DataDir: sdd,
	})
	wl := writelogger.New(writelogger.Config{
		DataDir: wdd,
	})

	mm := NewManagerManager(sn, wl, logger.NewStandardLogger(os.Stderr))

	qtid := dax.QualifiedTableID{
		TableQualifier: dax.TableQualifier{
			OrganizationID: dax.OrganizationID("org1"),
			DatabaseID:     dax.DatabaseID("db1"),
		},
		ID:   dax.TableID("blah"),
		Name: "blah",
	}

	// get a manager and perform normal startup routine on empty data
	mgr := mm.GetShardManager(qtid, dax.PartitionNum(1), dax.ShardNum(1))

	d, err := mgr.LoadLatestSnapshot()
	assert.NoError(t, err)
	n, err := d.Read(make([]byte, 8))
	assert.Equal(t, 0, n)
	assert.Equal(t, err, io.EOF)

	wld, err := mgr.LoadWriteLog()
	assert.NoError(t, err)
	n, err = wld.Read(make([]byte, 8))
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)

	err = mgr.Lock()
	assert.NoError(t, err)

	wld, err = mgr.LoadWriteLog()
	assert.NoError(t, err)
	n, err = wld.Read(make([]byte, 8))
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)

	// append some data
	err = mgr.Append([]byte("blahblah"))
	assert.NoError(t, err)

	mm2 := NewManagerManager(sn, wl, logger.NewStandardLogger(os.Stderr))
	// get second manager for same stuff
	mgr2 := mm2.GetShardManager(qtid, dax.PartitionNum(1), dax.ShardNum(1))
	// load snapshot on 2nd manager (empty)
	d, err = mgr2.LoadLatestSnapshot()
	assert.NoError(t, err)
	n, err = d.Read(make([]byte, 8))
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)

	// load WL on 2nd manager (blahblah)
	wld, err = mgr2.LoadWriteLog()
	assert.NoError(t, err)
	buf := make([]byte, 16)
	n, err = wld.Read(buf)
	assert.Equal(t, 9, n)
	assert.Equal(t, "blahblah\n", string(buf[:9]))
	n, err = wld.Read(buf)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)

	// begin snapshot procedure on 1st manager
	err = mgr.IncrementWLVersion()
	assert.NoError(t, err)

	// do append on 1st manager mid-snapshot
	err = mgr.Append([]byte("blahbla2"))
	assert.NoError(t, err)

	// snapshot 1st manager
	rc := io.NopCloser(bytes.NewBufferString("hahaha"))
	err = mgr.Snapshot(rc)
	assert.NoError(t, err)

	// append again on 1st manager
	err = mgr.Append([]byte("blahbla3"))
	assert.NoError(t, err)

	// locking 2nd manager should fail
	err = mgr2.Lock()
	assert.NotNil(t, err)

	// exit 1st manager
	err = mgr.Unlock()
	assert.NoError(t, err)

	// locking 2nd manager should succeed
	err = mgr2.Lock()
	assert.NoError(t, err)

	// loading write log should fail since there's been a snapshot
	// between the last load and locking.
	wld, err = mgr2.LoadWriteLog()
	assert.NotNil(t, err)

	// mgr2 dies due to error loading write lock
	err = mgr2.Unlock()
	assert.NoError(t, err)

	// get third manager for same stuff
	mm3 := NewManagerManager(sn, wl, logger.NewStandardLogger(os.Stderr))
	mgr3 := mm3.GetShardManager(qtid, dax.PartitionNum(1), dax.ShardNum(1))
	// load snapshot on 3nd manager
	d, err = mgr3.LoadLatestSnapshot()
	assert.NoError(t, err)
	buf = make([]byte, 6)
	n, err = d.Read(buf)
	assert.Equal(t, 6, n)
	assert.Equal(t, "hahaha", string(buf))
	assert.Equal(t, nil, err)

	// load write log on 3rd manager, get previous 2 writes
	wld, err = mgr3.LoadWriteLog()
	assert.NoError(t, err)
	buf = make([]byte, 20)
	n, _ = wld.Read(buf)
	assert.Equal(t, 18, n)
	assert.Equal(t, "blahbla2\nblahbla3\n", string(buf[:18]))
	n, err = wld.Read(buf)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)

	// lock 3rd manager
	err = mgr3.Lock()
	assert.NoError(t, err)

	// reload write log (should be empty)
	wld, err = mgr3.LoadWriteLog()
	assert.NoError(t, err)
	n, err = wld.Read(make([]byte, 8))
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}
