package client

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/disco"
	pnet "github.com/molecula/featurebase/v2/net"
	"github.com/molecula/featurebase/v2/shardwidth"
	"github.com/molecula/featurebase/v2/test"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var (
	testIndex                *Index
	testIndexWithKeys        *Index
	testIndexWithKeysNoTrack *Index
	testIndexAtomicRecord    *Index
	testIndexKeyTranslation  *Index

	testField            *Field
	testFieldTimeQuantum *Field
	testFieldInt0        *Field
	testFieldInt1        *Field
)

func setup(t *testing.T, cli *Client) {
	t.Helper()

	testSchema := NewSchema()
	testIndex = testSchema.Index("test-index")
	testIndexWithKeys = testSchema.Index("test-index-keys", OptIndexKeys(true))
	testIndexWithKeysNoTrack = testSchema.Index("test-index-keys-notrack",
		OptIndexKeys(true),
		OptIndexTrackExistence(false),
	)
	testField = testIndex.Field("test-field")
	testFieldTimeQuantum = testIndex.Field("test-field-timequantum", OptFieldTypeTime(TimeQuantumYear))
	testIndexKeyTranslation = testSchema.Index("test-index-key-translation", OptIndexKeys(true))

	testIndexAtomicRecord = testSchema.Index("test-index-atomic-record")
	testFieldInt0 = testIndexAtomicRecord.Field("test-field-int0", OptFieldTypeInt(-1000, 1000))
	testFieldInt1 = testIndexAtomicRecord.Field("test-field-int1", OptFieldTypeInt(-1000, 1000))

	require.NoErrorf(t, cli.SyncSchema(testSchema), "SyncSchema")
}

func tearDown(t *testing.T, cli *Client) {
	t.Helper()

	for _, i := range []*Index{testIndex, testIndexWithKeys, testIndexWithKeysNoTrack, testIndexAtomicRecord, testIndexKeyTranslation} {
		require.NoErrorf(t, cli.DeleteIndex(i), "DeleteIndex(%s)", i.name)
	}
}

func TestClientAgainstCluster(t *testing.T) {
	for size, replicaN := 3, 1; replicaN <= 2; replicaN++ {
		testName := fmt.Sprintf("%d.%d", size, replicaN)
		t.Run(testName, func(t *testing.T) {

			// Start size.replicaN cluster
			c := test.MustNewCluster(t, size)
			for _, n := range c.Nodes {
				n.Config.Cluster.ReplicaN = replicaN
			}
			err := c.Start()
			require.NoError(t, err, "Start cluster "+testName)

			urls := make([]string, len(c.Nodes))
			for i, n := range c.Nodes {
				urls[i] = n.URL()
			}
			defer c.Close()

			// Create a new client for the cluster
			cli, err := newClientFromAddresses(urls, &ClientOptions{})
			require.NoErrorf(t, err, "newClientFromAddresses(%v): %v", urls, err)
			defer cli.Close()

			t.Run("GetStatus", func(t *testing.T) {
				status, err := cli.Status()
				require.NoErrorf(t, err, "GET /status")

				require.Equalf(t, disco.ClusterStateNormal, disco.ClusterState(status.State), "GET /status")
			})

			t.Run("QueryRow", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				resp, err := cli.Query(testField.Row(1))
				require.NoErrorf(t, err, "Query Row")
				require.NotNil(t, resp, "Response should not be nil")
			})

			t.Run("QueryWithShards", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				shardWidth := uint64(1 << shardwidth.Exponent)

				_, err := cli.Query(testField.Set(1, 1))
				require.NoErrorf(t, err, "Set(1, %d)", 1)

				_, err = cli.Query(testField.Set(1, shardWidth))
				require.NoErrorf(t, err, "Set(1, %d)", shardWidth)

				_, err = cli.Query(testField.Set(1, shardWidth*3))
				require.NoErrorf(t, err, "Set(1, %d)", shardWidth*3)

				resp, err := cli.Query(testField.Row(1), OptQueryShards(0, 3))
				require.NoErrorf(t, err, "Row(1) OptQueryShards(0, 3)")

				cols := resp.Result().Row().Columns
				require.Equalf(t, []uint64{1, shardWidth * 3}, cols, "Unexpected results: %#v", cols)
			})

			t.Run("OrmCount", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldCount := testIndex.Field("test-field-count")
				err := cli.EnsureField(testFieldCount)
				require.NoError(t, err)

				qry := testIndex.BatchQuery(
					testFieldCount.Set(10, 20),
					testFieldCount.Set(10, 21),
					testFieldCount.Set(15, 25),
				)
				_, err = cli.Query(qry)
				require.NoErrorf(t, err, "BatchQuery")

				resp, err := cli.Query(testIndex.Count(testFieldCount.Row(10)))
				require.NoErrorf(t, err, "Count")
				require.Equalf(t, int64(2), resp.Result().Count(), "Count")
			})

			t.Run("DecimalField", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldDec := testIndex.Field("test-field-dec", OptFieldTypeDecimal(3))
				err := cli.EnsureField(testFieldDec)
				require.NoError(t, err)

				sch, err := cli.Schema()
				require.NoErrorf(t, err, "Schema")

				idx := sch.indexes[testIndex.name]
				opts := idx.Field(testFieldDec.name).Options()
				require.Equalf(t, int64(3), opts.scale, "%s scale", testFieldDec.name)
			})

			t.Run("IntersectReturns", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldSegments := testIndex.Field("test-field-segments")
				err := cli.EnsureField(testFieldSegments)
				require.NoError(t, err)

				qry1 := testIndex.BatchQuery(
					testFieldSegments.Set(2, 10),
					testFieldSegments.Set(2, 15),
					testFieldSegments.Set(3, 10),
					testFieldSegments.Set(3, 20),
				)
				_, err = cli.Query(qry1)
				require.NoErrorf(t, err, "BatchQuery")

				qry2 := testIndex.Intersect(testFieldSegments.Row(2), testFieldSegments.Row(3))
				resp, err := cli.Query(qry2)
				require.NoErrorf(t, err, "Intersect")

				require.Equalf(t, 1, len(resp.Results()), "Intersect number of results")
				require.Equalf(t, []uint64{10}, resp.Result().Row().Columns, "Intersect columns results")
			})

			t.Run("TopNReturns", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldTopN := testIndex.Field("test-field-topn")
				err := cli.EnsureField(testFieldTopN)
				require.NoError(t, err)

				qry := testIndex.BatchQuery(
					testFieldTopN.Set(10, 5),
					testFieldTopN.Set(10, 10),
					testFieldTopN.Set(10, 15),
					testFieldTopN.Set(20, 5),
					testFieldTopN.Set(30, 5),
				)
				_, err = cli.Query(qry)
				require.NoErrorf(t, err, "BatchQuery")

				// XXX: The following is required to make this test pass. See: https://github.com/molecula/featurebase/issues/625
				_, _, err = cli.HTTPRequest("POST", "/recalculate-caches", nil, nil)
				require.NoErrorf(t, err, "POST /recalculate-caches")

				resp, err := cli.Query(testFieldTopN.TopN(2))
				require.NoErrorf(t, err, "TopN(2)")

				items := resp.Result().CountItems()
				require.Equalf(t, 2, len(items), "TopN result CountItems")

				item := items[0]
				require.Equalf(t, uint64(10), item.ID, "TopN result item[0].ID")
				require.Equalf(t, uint64(3), item.Count, "TopN result item[0].Count")
			})

			t.Run("MinMaxRow", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldMinMax := testIndex.Field("test-field-minmax")
				err := cli.EnsureField(testFieldMinMax)
				require.NoError(t, err)

				qry := testIndex.BatchQuery(
					testFieldMinMax.Set(10, 5),
					testFieldMinMax.Set(10, 10),
					testFieldMinMax.Set(10, 15),
					testFieldMinMax.Set(20, 5),
					testFieldMinMax.Set(30, 5),
				)
				_, err = cli.Query(qry)
				require.NoErrorf(t, err, "Setting bits")

				resp, err := cli.Query(testFieldMinMax.MinRow())
				require.NoErrorf(t, err, "MinRow")

				min := resp.Result().CountItem().ID
				require.Equalf(t, uint64(10), min, "Min")

				resp, err = cli.Query(testFieldMinMax.MaxRow())
				require.NoErrorf(t, err, "MaxRow")

				max := resp.Result().CountItem().ID
				require.Equalf(t, uint64(30), max, "Max")
			})

			t.Run("SetMutexField", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldMutex := testIndex.Field("test-field-mutex", OptFieldTypeMutex(CacheTypeDefault, 0))
				err := cli.EnsureField(testFieldMutex)
				require.NoError(t, err)

				// can set mutex
				_, err = cli.Query(testFieldMutex.Set(1, 100))
				require.NoErrorf(t, err, "Set(1, 100)")

				resp, err := cli.Query(testFieldMutex.Row(1))
				require.NoErrorf(t, err, "Row(1)")

				target := []uint64{100}
				require.Equalf(t, target, resp.Result().Row().Columns, "Row Result Columns")

				// setting another row removes the previous
				_, err = cli.Query(testFieldMutex.Set(42, 100))
				require.NoErrorf(t, err, "Set(42, 100)")

				resp, err = cli.Query(testIndex.BatchQuery(
					testFieldMutex.Row(1),
					testFieldMutex.Row(42),
				))
				require.NoErrorf(t, err, "BatchQuery")

				target1 := []uint64(nil)
				target42 := []uint64{100}
				require.Equalf(t, target1, resp.Results()[0].Row().Columns, "Row Results[0] Columns")
				require.Equalf(t, target42, resp.Results()[1].Row().Columns, "Row Results[1] Columns")
			})

			t.Run("SetBoolField", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldBool := testIndex.Field("test-field-bool", OptFieldTypeBool())
				err := cli.EnsureField(testFieldBool)
				require.NoError(t, err)

				// can set bool
				_, err = cli.Query(testFieldBool.Set(true, 100))
				require.NoErrorf(t, err, "Set(true, 100)")

				resp, err := cli.Query(testFieldBool.Row(true))
				require.NoErrorf(t, err, "Row(true)")

				target := []uint64{100}
				require.Equalf(t, target, resp.Result().Row().Columns, "Row Result Columns")
			})

			t.Run("ClearRowQuery", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldClear := testIndex.Field("test-field-clear")
				err := cli.EnsureField(testFieldClear)
				require.NoError(t, err)

				_, err = cli.Query(testIndex.BatchQuery(
					testFieldClear.Set(1, 100),
					testFieldClear.Set(1, 200),
				))
				require.NoErrorf(t, err, "Set(1, 100) Set(1, 200)")

				resp, err := cli.Query(testFieldClear.Row(1))
				require.NoErrorf(t, err, "Row(1)")

				target := []uint64{100, 200}
				require.Equalf(t, target, resp.Result().Row().Columns, "Row Result Columns")

				_, err = cli.Query(testFieldClear.ClearRow(1))
				require.NoErrorf(t, err, "ClearRow(1)")

				resp, err = cli.Query(testFieldClear.Row(1))
				require.NoErrorf(t, err, "Row(1)")

				target = []uint64(nil)
				require.Equalf(t, target, resp.Result().Row().Columns, "Row Result Columns")
			})

			t.Run("RowsQuery", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldRows := testIndex.Field("test-field-rows")
				err := cli.EnsureField(testFieldRows)
				require.NoError(t, err)

				_, err = cli.Query(testIndex.BatchQuery(
					testFieldRows.Set(1, 100),
					testFieldRows.Set(1, 200),
					testFieldRows.Set(2, 200),
				))
				require.NoErrorf(t, err, "Set(1, 100) Set(1, 200) Set(2, 200)")

				resp, err := cli.Query(testFieldRows.Rows())
				require.NoErrorf(t, err, "Rows")

				target := RowIdentifiersResult{
					IDs: []uint64{1, 2},
				}
				require.Equalf(t, target, resp.Result().RowIdentifiers(), "RowIdentifiers Result")
			})

			t.Run("UnionRowsQuery", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldRows := testIndex.Field("test-field-rows")
				err := cli.EnsureField(testFieldRows)
				require.NoError(t, err)

				_, err = cli.Query(testIndex.BatchQuery(
					testFieldRows.Set(1, 100),
					testFieldRows.Set(1, 200),
					testFieldRows.Set(2, 200),
				))
				require.NoErrorf(t, err, "Set(1, 100) Set(1, 200) Set(2, 200)")

				resp, err := cli.Query(testFieldRows.Rows().Union())
				require.NoErrorf(t, err, "Rows Union")

				target := []uint64{100, 200}
				require.Equalf(t, target, resp.Result().Row().Columns, "Row Result Columns")
			})

			t.Run("LikeQuery", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldLike := testIndex.Field("test-field-like", OptFieldKeys(true))
				err := cli.EnsureField(testFieldLike)
				require.NoError(t, err)

				_, err = cli.Query(testIndex.BatchQuery(
					testFieldLike.Set("a", 100),
					testFieldLike.Set("b", 200),
					testFieldLike.Set("bc", 200),
				))
				require.NoErrorf(t, err, "Set(a, 100) Set(b, 200) Set(bc, 200)")

				resp, err := cli.Query(testFieldLike.Like("b%"))
				require.NoErrorf(t, err, `Like(b%)`)

				target := RowIdentifiersResult{
					Keys: []string{"b", "bc"},
				}
				require.Equalf(t, target, resp.Result().RowIdentifiers(), "RowIdentifiers Result")
			})

			t.Run("GroupByQuery", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldGroupBy := testIndex.Field("test-field-group-by")
				err := cli.EnsureField(testFieldGroupBy)
				require.NoError(t, err)

				_, err = cli.Query(testIndex.BatchQuery(
					testFieldGroupBy.Set(1, 100),
					testFieldGroupBy.Set(1, 200),
					testFieldGroupBy.Set(2, 200),
				))
				require.NoErrorf(t, err, "Set(1, 100) Set(1, 200) Set(2, 200)")

				resp, err := cli.Query(testIndex.GroupBy(testFieldGroupBy.Rows()))
				require.NoErrorf(t, err, `Like(b%)`)

				target := []GroupCount{
					{Groups: []FieldRow{{FieldName: "test-field-group-by", RowID: 1}}, Count: 2},
					{Groups: []FieldRow{{FieldName: "test-field-group-by", RowID: 2}}, Count: 1},
				}

				assertGroupBy(t, target, resp.Result().GroupCounts())
			})

			t.Run("GroupByQuery", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldGroupBy := testIndex.Field("test-field-group-by-int", OptFieldTypeInt(-10, 10))
				err := cli.EnsureField(testFieldGroupBy)
				require.NoError(t, err)

				_, err = cli.Query(testIndex.RawQuery(`
		Set(0,      test-field-group-by-int=1)
		Set(1,      test-field-group-by-int=2)

		Set(2,      test-field-group-by-int=-2)
		Set(3,      test-field-group-by-int=-1)

		Set(4,      test-field-group-by-int=4)

		Set(10,     test-field-group-by-int=0)
		Set(100,    test-field-group-by-int=0)
		Set(1000,   test-field-group-by-int=0)
		Set(10000,  test-field-group-by-int=0)
		Set(100000, test-field-group-by-int=0)
		`))
				require.NoError(t, err, "Set(0..100000)")

				resp, err := cli.Query(testIndex.GroupBy(testFieldGroupBy.Rows()))
				require.NoErrorf(t, err, `GroupBy(Rows)`)

				var a, b, c, d, e, f int64 = -2, -1, 0, 1, 2, 4
				target := []GroupCount{
					{Groups: []FieldRow{{FieldName: "test-field-group-by-int", Value: &a}}, Count: 1},
					{Groups: []FieldRow{{FieldName: "test-field-group-by-int", Value: &b}}, Count: 1},
					{Groups: []FieldRow{{FieldName: "test-field-group-by-int", Value: &c}}, Count: 5},
					{Groups: []FieldRow{{FieldName: "test-field-group-by-int", Value: &d}}, Count: 1},
					{Groups: []FieldRow{{FieldName: "test-field-group-by-int", Value: &e}}, Count: 1},
					{Groups: []FieldRow{{FieldName: "test-field-group-by-int", Value: &f}}, Count: 1},
				}
				assertGroupBy(t, target, resp.Result().GroupCounts())
			})

			t.Run("CreateDeleteIndexField", func(t *testing.T) {
				tmpIndex := NewIndex("tmp-index")
				tmpField := tmpIndex.Field("tmp-field")

				err := cli.CreateIndex(tmpIndex)
				require.NoError(t, err)

				err = cli.CreateField(tmpField)
				require.NoError(t, err)

				err = cli.DeleteField(tmpField)
				require.NoError(t, err)

				err = cli.DeleteIndex(tmpIndex)
				require.NoError(t, err)
			})

			t.Run("ErrorCreatingIndexField", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				require.ErrorIs(t, cli.CreateIndex(testIndex), ErrIndexExists)
				require.ErrorIs(t, cli.CreateField(testField), ErrFieldExists)
			})

			t.Run("Failover", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				uri, _ := pnet.NewURIFromAddress("does-not-resolve.foo.bar")
				tmpcli, _ := NewClient(NewClusterWithHost(uri, uri, uri, uri), OptClientRetries(0))

				_, err := tmpcli.Query(testIndex.All())
				require.Error(t, err, ErrTriedMaxHosts)
			})

			t.Run("InvalidQuery", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				_, _, err := cli.HTTPRequest("INVALID METHOD", "/foo", nil, nil)
				require.Error(t, err)

				_, err = cli.Query(testIndex.RawQuery("Invalid query"))
				require.Error(t, err)
			})

			t.Run("Sync", func(t *testing.T) {
				testIndexRemote := NewIndex("test-index-remote")
				err := cli.EnsureIndex(testIndexRemote)
				require.NoError(t, err)

				testFieldRemote := testIndexRemote.Field("test-field-remote")
				err = cli.EnsureField(testFieldRemote)
				require.NoError(t, err)

				schema := NewSchema()
				idx1 := schema.Index("index-1")
				idx1.Field("field-1-1")
				idx1.Field("field-1-2")

				idx2 := schema.Index("index-2")
				idx2.Field("field-2-1")
				schema.Index(testIndexRemote.Name())

				err = cli.SyncSchema(schema)
				require.NoError(t, err)

				err = cli.DeleteIndex(testIndexRemote)
				require.NoError(t, err)

				err = cli.DeleteIndex(idx1)
				require.NoError(t, err)

				err = cli.DeleteIndex(idx2)
				require.NoError(t, err)
			})

			t.Run("FetchFragmentNodes", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				nodes, err := cli.fetchFragmentNodes(testIndex.Name(), 0)
				require.NoErrorf(t, err, "fetchFragmentNodes(%s, 0)", testIndex.name)
				require.Equalf(t, replicaN, len(nodes), "len(nodes)")

				// running the same for coverage
				nodes, err = cli.fetchFragmentNodes(testIndex.Name(), 0)
				require.NoErrorf(t, err, "fetchFragmentNodes(%s, 0)", testIndex.name)
				require.Equalf(t, replicaN, len(nodes), "len(nodes)")
			})

			t.Run("RowRangeQuery", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldRange := testIndex.Field("test-field-range", OptFieldTypeTime(TimeQuantumMonthDayHour))
				err := cli.EnsureField(testFieldRange)
				require.NoError(t, err)

				_, err = cli.Query(testIndex.BatchQuery(
					testFieldRange.SetTimestamp(10, 100, time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)),
					testFieldRange.SetTimestamp(10, 100, time.Date(2018, time.January, 1, 0, 0, 0, 0, time.UTC)),
					testFieldRange.SetTimestamp(10, 100, time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC)),
				))
				require.NoErrorf(t, err, "BatchQuery SetTimestamp")

				start := time.Date(2017, time.January, 5, 0, 0, 0, 0, time.UTC)
				end := time.Date(2018, time.January, 5, 0, 0, 0, 0, time.UTC)
				resp, err := cli.Query(testFieldRange.RowRange(10, start, end))
				require.NoErrorf(t, err, "RowRange(10, %v, %v)", start, end)

				target := []uint64{100}
				require.Equalf(t, target, resp.Result().Row().Columns, "Row Result Columns")
			})
			t.Run("StoreQuery", func(t *testing.T) {
				schema := NewSchema()
				testIndexStore := schema.Index("test-index-store")
				testFieldFrom := testIndexStore.Field("test-field-from")
				testFieldTo := testIndexStore.Field("test-field-to")
				err := cli.SyncSchema(schema)
				require.NoError(t, err)

				defer func() {
					cerr := cli.DeleteIndex(testIndexStore)
					require.NoErrorf(t, cerr, "failed to delete index: %v", testIndexStore.name)
				}()

				_, err = cli.Query(testIndexStore.BatchQuery(
					testFieldFrom.Set(10, 100),
					testFieldFrom.Set(10, 200),
					testFieldTo.Store(testFieldFrom.Row(10), 1),
				))
				require.NoErrorf(t, err, "Set(10, 100) Set(10, 200) Store(Row(10), 1)")

				resp, err := cli.Query(testFieldTo.Row(1))
				require.NoErrorf(t, err, "Row(1)")

				target := []uint64{100, 200}
				require.Equalf(t, target, resp.Result().Row().Columns, "Row Result Columns")
			})

			t.Run("MultipleClientKeyQuery", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldMultiClient := testIndexWithKeys.Field("test-field-multiclient")
				err := cli.EnsureField(testFieldMultiClient)
				require.NoError(t, err)

				eg := &errgroup.Group{}
				for i := 0; i < 10; i++ {
					rowID := uint64(i)
					eg.Go(func() error {
						_, e := cli.Query(testFieldMultiClient.Set(rowID, "col"))
						return e
					})
				}
				require.NoError(t, eg.Wait())
			})

			t.Run("ExportRowIDColumnID", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldExport := testIndex.Field("test-field-export")
				err := cli.EnsureField(testFieldExport)
				require.NoError(t, err)

				_, err = cli.Query(testIndex.BatchQuery(
					testFieldExport.Set(1, 1),
					testFieldExport.Set(1, 10),
					testFieldExport.Set(2, 1048577),
				), nil)
				require.NoErrorf(t, err, "Set(1, 1) Set(1, 10) Set(2, 1048577)")

				r, err := cli.ExportField(testFieldExport)
				require.NoErrorf(t, err, "ExportField")

				b, err := ioutil.ReadAll(r)
				require.NoError(t, err)

				target := "1,1\n1,10\n2,1048577\n"
				require.Equalf(t, target, string(b), "Export Field Response")
			})

			t.Run("ExportRowIDColumnKey", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldExport := testIndexWithKeys.Field("test-field-export")
				err := cli.EnsureField(testFieldExport)
				require.NoError(t, err)

				_, err = cli.Query(testIndexWithKeys.BatchQuery(
					testFieldExport.Set(1, "one"),
					testFieldExport.Set(1, "ten"),
					testFieldExport.Set(2, "big-number"),
				), nil)
				require.NoErrorf(t, err, "Set(1, one) Set(1, ten) Set(2, big-number)")

				r, err := cli.ExportField(testFieldExport)
				require.NoErrorf(t, err, "ExportField")

				b, err := ioutil.ReadAll(r)
				require.NoError(t, err)

				target := "1,one\n1,ten\n2,big-number\n"
				require.Equalf(t, target, string(b), "Export Field Response")
			})

			t.Run("TranslateRowKeys", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				testFieldTranslate := testIndexKeyTranslation.Field("test-field-translate", OptFieldKeys(true))
				err := cli.EnsureField(testFieldTranslate)
				require.NoError(t, err)

				trans, err := cli.CreateFieldKeys(testFieldTranslate, "key1", "key2")
				require.NoErrorf(t, err, "CreateFieldKeys")

				target := map[string]uint64{"key1": 1, "key2": 2}
				require.Equalf(t, target, trans, "CreateFieldKeys")

				trans, err = cli.FindFieldKeys(testFieldTranslate, "key1", "key2", "key3")
				require.NoErrorf(t, err, "FindFieldKeys")

				require.Equalf(t, target, trans, "FindFieldKeys")
			})

			t.Run("TranslateColKeys", func(t *testing.T) {
				setup(t, cli)
				defer tearDown(t, cli)

				created, err := cli.CreateIndexKeys(testIndexKeyTranslation, "key1", "key2")
				require.NoErrorf(t, err, "CreateIndexKeys")
				if _, ok := created["key1"]; !ok {
					t.Error("key1 missing")
				}
				if _, ok := created["key2"]; !ok {
					t.Error("key2 missing")
				}

				found, err := cli.FindIndexKeys(testIndexKeyTranslation, "key1", "key2", "key3")
				require.NoErrorf(t, err, "FindIndexKeys")

				require.Equalf(t, created, found, "IndexKeys")
			})

			t.Run("Transactions", func(t *testing.T) {
				trns, err := cli.StartTransaction("blah", time.Minute, false, time.Minute)
				require.NoErrorf(t, err, "StartTransaction(blah)")
				require.Equalf(t, "blah", trns.ID, "TranslateColumnKeys ID")
				require.Equalf(t, time.Minute, trns.Timeout, "TranslateColumnKeys Timeout")
				require.Truef(t, trns.Active, "TranslateColumnKeys Active")

				trnsMap, err := cli.Transactions()
				require.NoErrorf(t, err, "Transactions")
				require.Equalf(t, 1, len(trnsMap), "Transactions len")
				require.Truef(t, trnsMap["blah"].Active, "Transactions Active")

				trns, err = cli.GetTransaction("blah")
				require.NoErrorf(t, err, "GetTransaction(blah)")
				require.Equalf(t, "blah", trns.ID, "TranslateColumnKeys ID")
				require.Equalf(t, time.Minute, trns.Timeout, "TranslateColumnKeys Timeout")
				require.Truef(t, trns.Active, "TranslateColumnKeys Active")

				trns, err = cli.FinishTransaction("blah")
				require.NoErrorf(t, err, "FinishTransaction(blah)")
				require.Equalf(t, "blah", trns.ID, "TranslateColumnKeys ID")
				require.Equalf(t, time.Minute, trns.Timeout, "TranslateColumnKeys Timeout")
				require.Truef(t, trns.Active, "TranslateColumnKeys Active")
			})
		})
	}
}

func assertGroupBy(t *testing.T, expected, results []GroupCount) {
	t.Helper()

	require.Equalf(t, len(expected), len(results), "number of groupings mismatch")

	for i, result := range results {
		require.Equalf(t, expected[i], result, "unexpected result at %d", i)
	}
}
