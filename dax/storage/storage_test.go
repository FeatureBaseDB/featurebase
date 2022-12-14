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

func TestResourceManager(t *testing.T) {
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

	mm := NewResourceManager(sn, wl, logger.NewStandardLogger(os.Stderr))

	qtid := dax.QualifiedTableID{
		TableQualifier: dax.TableQualifier{
			OrganizationID: dax.OrganizationID("org1"),
			DatabaseID:     dax.DatabaseID("db1"),
		},
		ID:   dax.TableID("blah"),
		Name: "blah",
	}
	var n int
	var d, wld io.ReadCloser

	// get a resource and perform normal startup routine on empty data
	resource := mm.GetShardResource(qtid, dax.PartitionNum(1), dax.ShardNum(1))

	d, err = resource.LoadLatestSnapshot()
	assert.NoError(t, err)
	assert.Nil(t, d)

	wld, err = resource.LoadWriteLog()
	assert.NoError(t, err)
	assert.Nil(t, wld)

	err = resource.Lock()
	assert.NoError(t, err)

	wld, err = resource.LoadWriteLog()
	assert.NoError(t, err)
	assert.Nil(t, wld)

	// append some data
	err = resource.Append([]byte("blahblah"))
	assert.NoError(t, err)

	// a new ResourceManager is necessary so we get a new Resource with
	// new internal state instead of a cached Resource.
	mm2 := NewResourceManager(sn, wl, logger.NewStandardLogger(os.Stderr))
	// get second resource for same stuff
	resource2 := mm2.GetShardResource(qtid, dax.PartitionNum(1), dax.ShardNum(1))
	// load snapshot on 2nd resource (empty)
	d, err = resource2.LoadLatestSnapshot()
	assert.NoError(t, err)
	assert.Nil(t, d)

	// load WL on 2nd resource (blahblah)
	wld, err = resource2.LoadWriteLog()
	assert.NoError(t, err)
	buf := make([]byte, 16)
	n, _ = wld.Read(buf)
	assert.Equal(t, 9, n)
	assert.Equal(t, "blahblah\n", string(buf[:9]))
	n, err = wld.Read(buf)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)

	// begin snapshot procedure on 1st resource
	err = resource.IncrementWLVersion()
	assert.NoError(t, err)

	// do append on 1st resource mid-snapshot
	err = resource.Append([]byte("blahbla2"))
	assert.NoError(t, err)

	// snapshot 1st resource
	rc := io.NopCloser(bytes.NewBufferString("hahaha"))
	err = resource.Snapshot(rc)
	assert.NoError(t, err)

	// append again on 1st resource
	err = resource.Append([]byte("blahbla3"))
	assert.NoError(t, err)

	// locking 2nd resource should fail
	err = resource2.Lock()
	assert.NotNil(t, err)

	// exit 1st resource
	err = resource.Unlock()
	assert.NoError(t, err)

	// locking 2nd resource should succeed
	err = resource2.Lock()
	assert.NoError(t, err)

	// loading write log should fail since there's been a snapshot
	// between the last load and locking.
	_, err = resource2.LoadWriteLog()
	assert.NotNil(t, err)

	// resource2 dies due to error loading write lock
	err = resource2.Unlock()
	assert.NoError(t, err)

	// get third resource for same stuff
	mm3 := NewResourceManager(sn, wl, logger.NewStandardLogger(os.Stderr))
	resource3 := mm3.GetShardResource(qtid, dax.PartitionNum(1), dax.ShardNum(1))
	// load snapshot on 3nd resource
	d, err = resource3.LoadLatestSnapshot()
	assert.NoError(t, err)
	buf = make([]byte, 6)
	n, err = d.Read(buf)
	assert.Equal(t, 6, n)
	assert.Equal(t, "hahaha", string(buf))
	assert.Equal(t, nil, err)

	// load write log on 3rd resource, get previous 2 writes
	wld, err = resource3.LoadWriteLog()
	assert.NoError(t, err)
	buf = make([]byte, 20)
	n, _ = wld.Read(buf)
	assert.Equal(t, 18, n)
	assert.Equal(t, "blahbla2\nblahbla3\n", string(buf[:18]))
	n, err = wld.Read(buf)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)

	// lock 3rd resource
	err = resource3.Lock()
	assert.NoError(t, err)

	// reload write log (should be empty)
	wld, err = resource3.LoadWriteLog()
	assert.NoError(t, err)
	n, err = wld.Read(make([]byte, 8))
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}
