package idk

// comment
import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/featurebasedb/featurebase/v3/authn"
	pilosaclient "github.com/featurebasedb/featurebase/v3/client"
	"github.com/featurebasedb/featurebase/v3/idk/idktest"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
)

func configureTestFlags(main *Main) {
	if pilosaHost, ok := os.LookupEnv("IDK_TEST_PILOSA_HOST"); ok {
		main.PilosaHosts = []string{pilosaHost}
	} else {
		main.PilosaHosts = []string{"pilosa:10101"}
	}
	if grpcHost, ok := os.LookupEnv("IDK_TEST_PILOSA_GRPC_HOST"); ok {
		main.PilosaGRPCHosts = []string{grpcHost}
	} else {
		main.PilosaGRPCHosts = []string{"pilosa:20101"}
	}

	main.Stats = ""
}

func TestErrFlush(t *testing.T) {
	ts := newTestSource([]Field{IDField{NameVal: "aval"}},
		[][]interface{}{
			{ErrFlush},
			{ErrFlush},
			{1},
			{1},
			{ErrFlush},
			{1},
			{1},
		})

	ingester := NewMain()
	ingester.AutoGenerate = true
	configureTestFlags(ingester)
	ingester.NewSource = func() (Source, error) { return ts, nil }
	rand.Seed(time.Now().UTC().UnixNano())
	ingester.Index = fmt.Sprintf("errflush%d", rand.Intn(10000000))
	ingester.BatchSize = 10
	ingester.CommitTimeout = 1 * time.Minute

	err := ingester.Run()
	if err != nil {
		t.Fatalf("%s: %v", idktest.ErrRunningIngest, err)
	}

	client := ingester.PilosaClient()
	defer func() {
		if err := client.DeleteIndexByName(ingester.Index); err != nil {
			t.Fatal(err)
		}
	}()

	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	index := schema.Index(ingester.Index)
	qr, err := client.Query(index.RawQuery("Count(Row(aval=1))"))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if qr.Results()[0].Count() != 4 {
		t.Fatalf("unexpected number of records: %d", qr.Results()[0].Count())
	}
}

func TestErrBatchNowStale(t *testing.T) {
	schema := []Field{IDField{NameVal: "id"}}

	records := make(chan []interface{})
	committed := make(chan []interface{}, 5)
	ts := newSignalingTestSource(schema, records, committed)

	ingester := NewMain()
	ingester.BatchMaxStaleness = time.Millisecond
	configureTestFlags(ingester)
	ingester.NewSource = func() (Source, error) { return ts, nil }
	ingester.AutoGenerate = true
	rand.Seed(time.Now().UTC().UnixNano())
	ingester.Index = fmt.Sprintf("batchnowstaletest%d", rand.Intn(100000))
	ingester.BatchSize = 5

	defer func() {
		if err := ingester.PilosaClient().DeleteIndexByName(ingester.Index); err != nil {
			t.Logf("%s for index %s: %v", idktest.ErrDeletingIndex, ingester.Index, err)
		}
	}()

	signal := make(chan struct{})
	go func() {
		err := ingester.Run()
		if err != nil {
			t.Logf("%s: %v", idktest.ErrRunningIngest, err)
		}
		close(signal)
	}()

	records <- []interface{}{1}
	records <- []interface{}{2}
	time.Sleep(time.Millisecond)
	records <- []interface{}{3}

	select {
	case <-committed:
	case <-time.After(time.Second):
		t.Fatal("Nothing has been committed")
	}

	close(records)

	// make sure the Run goroutine actually finished
	<-signal
}

func TestIngestSignedIntBoolField(t *testing.T) {

	ts := newTestSource([]Field{StringField{NameVal: "rcid"}, SignedIntBoolKeyField{NameVal: "svals"}},
		[][]interface{}{
			{"a", int64(-22)},
			{"a", int64(-44)},
			{"a", int64(11)},
			{"a", int64(5)},
			{"a", int64(66)},
			{"b", int64(11)},
			{"b", int64(22)},
			{"b", int64(-32)},
			{"b", int64(-44)},
			{"b", int64(9)},
			{"b", int64(7)},
			{"c", int64(5)},
		})

	ingester := NewMain()
	configureTestFlags(ingester)
	ingester.NewSource = func() (Source, error) { return ts, nil }
	rand.Seed(time.Now().UTC().UnixNano())
	ingester.Index = fmt.Sprintf("ingestint%d", rand.Intn(100000))
	ingester.BatchSize = 2
	ingester.PrimaryKeyFields = []string{"rcid"}

	err := ingester.Run()
	if err != nil {
		t.Fatalf("%s: %v", idktest.ErrRunningIngest, err)
	}

	client := ingester.PilosaClient()
	defer func() {
		if err := client.DeleteIndexByName(ingester.Index); err != nil {
			t.Fatal(err)
		}
	}()

	svals := ingester.index.Field("svals")
	svalsex := ingester.index.Field("svals-exists")

	tests := []struct {
		field *pilosaclient.Field
		row   int
		want  []string
	}{
		{field: svals, row: 22, want: []string{"b"}},
		{field: svalsex, row: 22, want: []string{"a", "b"}},
		{field: svalsex, row: 44, want: []string{"a", "b"}},
		{field: svalsex, row: 5, want: []string{"a", "c"}},
		{field: svals, row: 5, want: []string{"a", "c"}},
	}

	for _, test := range tests {
		if resp, err := client.Query(test.field.Row(test.row)); err != nil {
			t.Fatalf("row %d: %v", test.row, err)
		} else if !stringSliceSame(resp.ResultList[0].Row().Keys, test.want) {
			t.Fatalf("wanted %+v, got: %+v", test.want, resp.ResultList[0].Row().Keys)
		}
	}
}

// TestSingleBoolClear essentially creates an import batch which
// clears a bit in a particular fragment without setting a bit in that
// same fragment.  There's a potential optimization in the pilosa client
// batch import code that will break this test if not done carefully.
func TestSingleBoolClear(t *testing.T) {
	ts := newTestSource([]Field{IDField{NameVal: "id"}, BoolField{NameVal: "likes_chocolate"}},
		[][]interface{}{
			{0, true},
		})

	ingester := NewMain()
	configureTestFlags(ingester)
	ingester.NewSource = func() (Source, error) { return ts, nil }
	rand.Seed(time.Now().UTC().UnixNano())
	ingester.Index = fmt.Sprintf("single_bool_clear%d", rand.Intn(100000))
	ingester.BatchSize = 1
	ingester.IDField = "id"

	if err := ingester.Run(); err != nil {
		t.Fatalf("%s: %v", idktest.ErrRunningIngest, err)
	}

	client := ingester.PilosaClient()
	defer func() {
		if err := client.DeleteIndexByName(ingester.Index); err != nil {
			t.Fatal(err)
		}
	}()

	bools := ingester.index.Field("bools")
	//boolsExists := ingester.index.Field("bools-exists")

	if resp, err := client.Query(bools.Row("likes_chocolate")); err != nil {
		t.Fatalf("likes_chocolate: %v", err)
	} else if resp.ResultList[0].Row().Columns[0] != 0 {
		t.Fatalf("wanted [0], got: %+v", resp.ResultList[0].Row().Columns)
	}

	ts2 := newTestSource([]Field{IDField{NameVal: "id"}, BoolField{NameVal: "likes_chocolate"}},
		[][]interface{}{
			{0, false},
		})

	ingester2 := NewMain()
	configureTestFlags(ingester2)
	ingester2.NewSource = func() (Source, error) { return ts2, nil }
	ingester2.Index = ingester.Index
	ingester2.IDField = "id"

	if err := ingester2.Run(); err != nil {
		t.Fatalf("running ingester2: %v", err)
	}

	if resp, err := client.Query(bools.Row("likes_chocolate")); err != nil {
		t.Fatalf("likes_chocolate: %v", err)
	} else if len(resp.ResultList[0].Row().Columns) != 0 {
		t.Fatalf("wanted [], got: %+v", resp.ResultList[0].Row().Columns)
	}
}

func TestForeignKeyRegression(t *testing.T) {
	ts := newTestSource([]Field{IDField{NameVal: "id"}, IntField{NameVal: "fk1", ForeignIndex: "testusers834"}},
		[][]interface{}{
			{0, "blah"},
			{1, "bleh"},
			{2, "blue"},
		})
	ingester := NewMain()
	configureTestFlags(ingester)
	ingester.NewSource = func() (Source, error) { return ts, nil }
	rand.Seed(time.Now().UTC().UnixNano())
	ingester.Index = fmt.Sprintf("fkreg%d", rand.Intn(100000))
	ingester.BatchSize = 2
	ingester.IDField = "id"

	defer func() {
		if err := ingester.PilosaClient().DeleteIndexByName(ingester.Index); err != nil {
			t.Logf("%s for index %s: %v", idktest.ErrDeletingIndex, ingester.Index, err)
		}
	}()

	onFinishRun, err := ingester.Setup()
	if err != nil {
		t.Fatalf("setting up ingester: %v", err)
	}
	defer onFinishRun()

	client := ingester.PilosaClient()
	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	index := schema.Index("testusers834", pilosaclient.OptIndexKeys(true))
	err = client.SyncIndex(index)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	defer func() {
		if err := client.DeleteIndexByName("testusers834"); err != nil {
			t.Logf("%s for index %s: %v", idktest.ErrDeletingIndex, "testusers834", err)
		}
	}()

	// this previously panicked because Batch.toTranslate wasn't
	// getting cleared for int fields.
	err = ingester.run()
	if err != nil {
		t.Fatalf("%s: %v", idktest.ErrRunningIngest, err)
	}

}

// Test String Arrays w/ various ingester configurations
func TestIngestStringArrays(t *testing.T) {
	// TODO: test with IDField
	ts := *newTestSource(
		[]Field{
			StringField{NameVal: "aba"},
			StringField{NameVal: "db"},
			IntField{NameVal: "id"},
			StringArrayField{NameVal: "sa1"},
		},
		[][]interface{}{
			{"a", "b", 1, []string{"a", "b", "c"}},
			{"a", "b", 1, []string{"z"}},
			{"a", "b", 1, []string{}},
			{"a", "b", 1, []string{"q", "r", "s", "t"}},
			{"a", "b", 1, nil},
			{"a", "b", 1, []string{"a", "b", "c"}},
			{"a", "b", 1, []string{"a", "b", "c"}},
			{"a", "b", 1, []string{"z"}},
			{"a", "b", 1, []string{}},
			{"a", "b", 1, []string{"q", "r", "s", "t"}},
			{"a", "b", 1, nil},
			{"a", "b", 1, []string{"a", "b", "c"}},
		},
	)
	certPath, ok := os.LookupEnv("IDK_TEST_CERT_PATH")
	if !ok {
		certPath = "/certs"
	}

	for _, tt := range []struct {
		name     string
		ingester func() *Main
		source   testSource
	}{
		{
			name: "PrimaryKeyFields",
			ingester: func() *Main {
				ingester := NewMain()
				configureTestFlags(ingester)
				ingester.PrimaryKeyFields = []string{"aba", "db", "id"}
				return ingester
			},
			source: ts,
		},
		{
			name: "External-Generate+TLS",
			ingester: func() *Main {
				ingester := NewMain()
				configureTestFlags(ingester)
				ingester.AutoGenerate = true
				ingester.ExternalGenerate = true
				ingester.TLS = TLSConfig{
					CertificatePath:          certPath + "/theclient.crt",
					CertificateKeyPath:       certPath + "/theclient.key",
					CACertPath:               certPath + "/ca.crt",
					EnableClientVerification: true,
				}
				return ingester
			},
			source: ts,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ingester := tt.ingester()
			ingester.NewSource = func() (Source, error) { return &tt.source, nil }
			rand.Seed(time.Now().UTC().UnixNano())
			ingester.Index = fmt.Sprintf("ingeststring%d", rand.Intn(100000))
			ingester.BatchSize = 5
			defer func() {
				if err := ingester.PilosaClient().DeleteIndexByName(ingester.Index); err != nil {
					t.Logf("%s for index %s: %v", idktest.ErrDeletingIndex, ingester.index, err)
				}
			}()
			err := ingester.Run()
			if err != nil {
				t.Fatalf("%s: %v", idktest.ErrRunningIngest, err)
			}

			client := ingester.PilosaClient()
			sa1 := ingester.index.Field("sa1")

			_, err = client.Query(sa1.Row("a"))
			if err != nil {
				t.Fatalf("row a: %v", err)
			}

		})
	}

}

func TestIngesterServesPrometheusEndpoint(t *testing.T) {
	schema := []Field{
		StringField{NameVal: "aba"},
		StringField{NameVal: "db"},
		IntField{NameVal: "id"},
		StringArrayField{NameVal: "sa1"},
	}

	records := make(chan []interface{})
	var committed chan []interface{} // make nil channel
	ts := newSignalingTestSource(schema, records, committed)

	ingester := NewMain()
	configureTestFlags(ingester)
	ingester.NewSource = func() (Source, error) { return ts, nil }
	ingester.PrimaryKeyFields = []string{"aba", "db", "id"}
	rand.Seed(time.Now().UTC().UnixNano())
	ingester.Index = fmt.Sprintf("statstest%d", rand.Intn(100000))
	ingester.BatchSize = 5
	ingester.Stats = "localhost:9093"
	signal := make(chan struct{})
	go func() {
		err := ingester.Run()
		if err != nil {
			t.Logf("%s: %v", idktest.ErrRunningIngest, err)
		}
		close(signal)
	}()
	records <- []interface{}{"a", "b", 1, []string{"a", "b", "c"}}

	// Try to read stats directly
	url := "http://" + ingester.Stats + "/metrics"
	response, err := http.Get(url)
	if err != nil {
		t.Errorf("request error: %v", err)
	}
	contents, err := ioutil.ReadAll(response.Body)
	defer response.Body.Close()
	if err != nil {
		t.Errorf("read error: %v", err)
	}
	if strings.Contains(string(contents), MetricIngesterRowsAdded) {
		t.Errorf("metric name missing: %v", MetricIngesterRowsAdded)
	}
	close(records)

	// make sure the Run goroutine actually finished
	<-signal
}

func TestDelete(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	indexName := fmt.Sprintf("delete%d", rand.Intn(100000))
	primaryKeyFields := []string{"aba", "db", "id"}

	tsWrite := newTestSource(
		[]Field{
			StringField{NameVal: "aba"},
			StringField{NameVal: "db"},
			IntField{NameVal: "id"},
			StringArrayField{NameVal: "sa1"},
			StringField{NameVal: "s1"},
			StringField{NameVal: "s2", Mutex: true},
			IntField{NameVal: "i1"},
			DecimalField{NameVal: "d1"},
			BoolField{NameVal: "bools|b1"},
		},
		[][]interface{}{
			{"a", "b", 2, []string{"d", "e", "f"}, "v", "w", 14, 15.8, true},
			{"a", "b", 3, []string{"d", "e", "f"}, "v", "w'll", 14, 15.8, true},
		},
	)

	// first set up deleter - we want to make sure that even if it starts first before the index is created that things still work
	tsDelete := newTestSource(
		[]Field{
			//IDField{NameVal: "id"},
			StringField{NameVal: "aba"},
			StringField{NameVal: "db"},
			IntField{NameVal: "id"},
			StringArrayField{NameVal: "fields"},
		},
		[][]interface{}{
			// {primarykey1, primarykey2, primarykey3, []fieldName}
			{"a", "b", 2, []string{"sa1"}},
			{"a", "b", 2, []string{"s1"}},
			{"a", "b", 2, []string{"s2"}},
			{"a", "b", 3, []string{"s2"}}, // regression - try deleting value with single quote
			{"a", "b", 2, []string{"i1"}},
			{"a", "b", 2, []string{"d1"}},
			{"a", "b", 1, []string{"bools|b1"}},
			// try deleting same fields again
			{"a", "b", 2, []string{"sa1"}},
			{"a", "b", 2, []string{"s1"}},
			{"a", "b", 2, []string{"s2"}},
			{"a", "b", 2, []string{"i1"}},
			{"a", "b", 2, []string{"d1"}},
			{"a", "b", 1, []string{"bools|b1"}},
			// try some bad messages for sanity check
			{"a", "b", 2, []string{}},
			//{"a", "b", 1, []string{""}},
			//{"a", "b", 1, []string{"not-exist"}},
			// {"a", "b", []string{"s1"}},
			// {4, nil},
		},
	)
	deleter := NewMain()
	configureTestFlags(deleter)
	deleter.Delete = true
	deleter.NewSource = func() (Source, error) { return tsDelete, nil }
	deleter.Index = indexName
	deleter.BatchSize = 5
	deleter.PrimaryKeyFields = []string{"aba", "db", "id"}

	onFinishRun, err := deleter.Setup()
	if err != nil {
		t.Fatalf("setting up deleter: %v", err)
	}
	defer onFinishRun()

	ingester := NewMain()
	configureTestFlags(ingester)
	ingester.NewSource = func() (Source, error) { return tsWrite, nil }
	ingester.PrimaryKeyFields = primaryKeyFields
	ingester.Index = indexName
	ingester.BatchSize = 1

	defer func() {
		if err := ingester.PilosaClient().DeleteIndexByName(ingester.Index); err != nil {
			t.Logf("%s for index %s: %v", idktest.ErrDeletingIndex, ingester.Index, err)
		}
	}()

	err = ingester.Run()
	if err != nil {
		t.Fatalf("%s: %v", idktest.ErrRunningIngest, err)
	}

	client := ingester.PilosaClient()
	sa1 := ingester.index.Field("sa1")
	s2 := ingester.index.Field("s2")

	if resp, err := client.Query(sa1.Row("d")); err != nil {
		t.Fatalf("row sa1=d: %v", err)
	} else if keys := resp.Result().Row().Keys; !stringSliceSame(keys, []string{"a|b|2", "a|b|3"}) {
		t.Errorf("exp: %v\ngot: %v", "[a|b|2 a|b|3]", keys)
	}

	if resp, err := client.Query(s2.Row("w'll")); err != nil {
		t.Fatalf("row a: %v", err)
	} else if keys := resp.Result().Row().Keys; !stringSliceSame(keys, []string{"a|b|3"}) {
		t.Errorf("unexpected keys for s2: %v", resp.Result().Row().Keys)
	}

	if err := deleter.run(); err != nil && errors.Cause(err) != io.EOF {
		t.Fatalf("running deleter: %v", err)
	}

	if resp, err := client.Query(sa1.Row("d")); err != nil {
		t.Fatalf("row a: %v", err)
	} else if keys := resp.Result().Row().Keys; len(keys) != 1 || keys[0] != "a|b|3" {
		t.Errorf("exp: %v\ngot: %v", "[a|b|3]", keys)
	}

	if resp, err := client.Query(s2.Row("w'll")); err != nil {
		t.Fatalf("s2 row w'll: %v", err)
	} else if len(resp.Result().Row().Keys) != 0 {
		t.Errorf("unexpected keys for s2 w'll: %v", resp.Result().Row().Keys)
	}

}

func TestGetPrimaryKeyRecordizer(t *testing.T) {
	tests := []struct {
		name     string
		schema   []Field
		pkFields []string
		expErr   string
		expSkip  map[int]struct{}
		rawRec   []interface{}
		expID    interface{}
	}{
		{
			name:   "no schema",
			expErr: "can't call getPrimaryKeyRecordizer with empty schema",
		},
		{
			name:   "no pkfields",
			schema: []Field{StringField{}},
			expErr: "can't call getPrimaryKeyRecordizer with empty pkFields",
		},
		{
			name:     "primary is StringArray",
			schema:   []Field{StringArrayField{NameVal: "blah"}},
			pkFields: []string{"blah"},
			expErr:   "field blah cannot be a primary key field because it is a StringArray field.",
		},
		{
			name:     "primary is StringArray complex",
			schema:   []Field{StringField{NameVal: "zaa"}, IntField{NameVal: "hey"}, StringArrayField{NameVal: "blah"}},
			pkFields: []string{"blah", "zaa"},
			expErr:   "field blah cannot be a primary key field because it is a StringArray field.",
		},
		{
			name:     "unknown pkfield",
			schema:   []Field{StringField{NameVal: "zaa"}},
			pkFields: []string{"zaa", "zz"},
			expErr:   "no field with primary key field name zz found",
		},
		{
			name:     "unknown pkfield complex",
			schema:   []Field{StringField{NameVal: "zaa"}, IntField{NameVal: "hey"}, StringField{NameVal: "blah"}},
			pkFields: []string{"blah", "zz", "zaa"},
			expErr:   "no field with primary key field name zz found",
		},
		{
			name:     "skip primary",
			schema:   []Field{StringField{NameVal: "a"}, IntField{NameVal: "b"}},
			pkFields: []string{"a"},
			expSkip:  map[int]struct{}{0: {}},
			rawRec:   []interface{}{"a", 9},
			expID:    "a",
		},
		{
			name:     "primaries as ints",
			schema:   []Field{StringField{NameVal: "a"}, IntField{NameVal: "b"}, IntField{NameVal: "c"}, IntField{NameVal: "d"}},
			pkFields: []string{"c", "d", "b"},
			rawRec:   []interface{}{"a", uint32(1), uint32(2), uint32(4)},
			expID:    []byte("2|4|1"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rdz, skips, err := getPrimaryKeyRecordizer(test.schema, test.pkFields)
			if test.expErr != "" {
				if err == nil {
					t.Fatalf("nil err, expected %s", test.expErr)
				}
				if !strings.Contains(err.Error(), test.expErr) {
					t.Fatalf("unmatched errs exp/got\n%s\n%v", test.expErr, err)
				}
				return
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !reflect.DeepEqual(skips, test.expSkip) {
				t.Errorf("unmatched skips exp/got\n%+v\n%+v", test.expSkip, skips)
			}

			row := &pilosaclient.Row{}
			err = rdz(test.rawRec, row)
			if err != nil {
				t.Fatalf("unexpected error from recordizer: %v", err)
			}
			if !reflect.DeepEqual(test.expID, row.ID) {
				t.Fatalf("mismatched row IDs exp: %+v, got: %+v", test.expID, row.ID)
			}

		})
	}
}

type serverInfo struct {
	Name        string
	AuthToken   string
	PilosaHosts []string
}

func TestBatchFromSchema(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	type testcase struct {
		name            string
		schema          []Field
		IDField         string
		pkFields        []string
		autogen         bool
		extgen          bool
		packBools       string
		rawRec          []interface{}
		rowID           interface{}
		rowVals         []interface{}
		err             string
		batchErr        string
		rdzErrs         []string
		time            pilosaclient.QuantizedTime
		lookupWriteIdxs []int
	}
	getQuantizedTime := func(t time.Time) pilosaclient.QuantizedTime {
		qt := pilosaclient.QuantizedTime{}
		qt.Set(t)
		return qt
	}
	runTest := func(t *testing.T, test testcase, removeIndex bool, server serverInfo) {
		m := NewMain()
		configureTestFlags(m)
		m.Index = "cmd_test_index23lkjdkfjr2"
		m.PrimaryKeyFields = test.pkFields
		m.IDField = test.IDField
		m.AutoGenerate = test.autogen
		m.ExternalGenerate = test.extgen
		m.PackBools = test.packBools
		m.BatchSize = 2
		m.Pprof = ""
		m.NewSource = func() (Source, error) { return nil, nil }
		if server.AuthToken != "" {
			m.AuthToken = server.AuthToken
		}
		m.PilosaHosts = server.PilosaHosts
		if removeIndex {
			defer func() {
				err := m.client.DeleteIndex(m.index)
				if err != nil {
					t.Logf("%s for index %s: %v", idktest.ErrDeletingIndex, m.Index, err)
				}
			}()
		}
		onFinishRun, err := m.Setup()
		if strings.Contains("validation", test.name) && testErr(t, test.err, err) {
			return
		}
		if err != nil {
			t.Fatalf("%v", err)
		}
		defer onFinishRun()

		rdzs, batch, row, lookupWriteIdxs, err := m.batchFromSchema(test.schema)
		if testErr(t, test.err, err) {
			return
		}
		for k, rdz := range rdzs {
			err = rdz(test.rawRec, row)
			if test.rdzErrs != nil && test.rdzErrs[k] != "" && testErr(t, test.rdzErrs[k], err) {
				return
			}
		}

		if !reflect.DeepEqual(row.ID, test.rowID) {
			t.Fatalf("row IDs exp: %+v got %+v", test.rowID, row.ID)
		}
		if !reflect.DeepEqual(row.Time, test.time) {
			t.Fatalf("times not equal: exp: %+v, got: %+v", test.time, row.Time)
		}
		if !reflect.DeepEqual(row.Values, test.rowVals) {
			t.Errorf("row values exp/got:\n%+v %[1]T\n%+v %[2]T", test.rowVals, row.Values)
			if len(row.Values) == len(test.rowVals) {
				for i, v := range row.Values {
					if !reflect.DeepEqual(v, test.rowVals[i]) {
						t.Errorf("%v %[1]T != %v %[2]T", test.rowVals[i], v)
					}
				}
			}
			t.Fail()
		}
		if test.lookupWriteIdxs != nil && !reflect.DeepEqual(lookupWriteIdxs, test.lookupWriteIdxs) {
			t.Fatalf("lookupWriteIdxs exp: %+v got %+v", test.lookupWriteIdxs, lookupWriteIdxs)
		}

		if test.autogen {
			if row.ID != nil {
				t.Fatalf("expected ID not to be set when autogen is used (because it gets set from nexter in the ingest loop which isn't called in this test.)")
			}
			row.ID = uint64(1) // set so Add doesn't panic
		}

		err = batch.Add(*row)
		if testErr(t, test.batchErr, err) {
			return
		}
	}

	tests := []testcase{
		{
			name: "validation",
			err:  "must set exactly one of --primary-key-field <fieldnames>, --id-field <fieldname>, --auto-generate",
		},
		{
			name:    "empty",
			autogen: true,
			err:     "can't batch with no fields or batch size",
		},
		{
			name:    "empty-w/ExtGen",
			autogen: true,
			extgen:  true,
			err:     "can't batch with no fields or batch size",
		},
		{
			name:    "no id field",
			schema:  []Field{StringField{}},
			IDField: "nope",
			err:     "ID field nope not found",
		},
		{
			name:     "pk error",
			pkFields: []string{"zoop"},
			err:      "getting primary key recordizer",
		},
		{
			name:      "pack bools",
			schema:    []Field{BoolField{NameVal: "a"}, IDField{NameVal: "b"}, BoolField{NameVal: "c"}},
			IDField:   "b",
			packBools: "bff",
			rawRec:    []interface{}{true, uint64(7), false},
			rowID:     uint64(7),
			rowVals:   []interface{}{"a", "a", nil, "c"},
		},
		{
			name:    "don't pack bools",
			schema:  []Field{BoolField{NameVal: "a"}, IDField{NameVal: "b"}, BoolField{NameVal: "c"}},
			IDField: "b",
			rawRec:  []interface{}{true, uint64(7), false},
			rowID:   uint64(7),
			rowVals: []interface{}{true, false},
			err:     "field type 'bool' is not currently supported through Batch",
		},
		{
			name:    "mutex field",
			schema:  []Field{StringField{NameVal: "a", Mutex: true}, IDField{NameVal: "b"}},
			IDField: "b",
			rawRec:  []interface{}{"aval", uint64(7)},
			rowID:   uint64(7),
			rowVals: []interface{}{"aval"},
		},
		{
			name:    "mutex field with time quantum",
			schema:  []Field{StringField{NameVal: "a", Mutex: true, Quantum: "YM"}, IDField{NameVal: "b"}},
			IDField: "b",
			rawRec:  []interface{}{"aval", uint64(7)},
			rowID:   uint64(7),
			err:     "can't specify a time quantum on a string mutex field",
		},
		{
			name:    "ttl",
			schema:  []Field{StringField{NameVal: "a", Mutex: false, Quantum: "YM", TTL: "0s"}, IDField{NameVal: "b"}},
			IDField: "b",
			rawRec:  []interface{}{"ttl", uint64(7)},
			rowID:   uint64(7),
			rowVals: []interface{}{"ttl"},
		},
		{
			name:    "bad ttl",
			schema:  []Field{StringField{NameVal: "a", Mutex: false, Quantum: "YM", TTL: "bad-ttl"}, IDField{NameVal: "b"}},
			IDField: "b",
			rawRec:  []interface{}{"b", uint64(7)},
			rowID:   uint64(7),
			rowVals: []interface{}{"ttl"},
			err:     "unable to parse TTL from field",
		},
		{
			name:     "string array field",
			schema:   []Field{StringArrayField{NameVal: "a"}, StringField{NameVal: "b"}},
			pkFields: []string{"b"},
			rawRec:   []interface{}{[]string{"aval", "aval2"}, uint64(7)},
			rowID:    []byte("7"),
			rowVals:  []interface{}{[]string{"aval", "aval2"}},
		},
		{
			name:     "id array field",
			schema:   []Field{IDArrayField{NameVal: "a"}, StringField{NameVal: "b"}},
			pkFields: []string{"b"},
			rawRec:   []interface{}{"1,2", uint64(7)},
			rowID:    []byte("7"),
			rowVals:  []interface{}{[]uint64{1, 2}},
		},
		{
			name:     "id array field brackets",
			schema:   []Field{IDArrayField{NameVal: "a"}, StringField{NameVal: "b"}},
			pkFields: []string{"b"},
			rawRec:   []interface{}{"[1,2]", uint64(7)},
			rowID:    []byte("7"),
			rowVals:  []interface{}{[]uint64{1, 2}},
		},
		{
			name:     "decimal field",
			schema:   []Field{StringField{NameVal: "a"}, DecimalField{NameVal: "b", Scale: 2}},
			pkFields: []string{"a"},
			rawRec:   []interface{}{"blah", uint64(321)},
			rowID:    "blah",
			rowVals:  []interface{}{int64(32100)},
		},
		{
			name:     "decimal field scale too small",
			schema:   []Field{StringField{NameVal: "a"}, DecimalField{NameVal: "b", Scale: -2}},
			pkFields: []string{"a"},
			err:      "scale values outside the range",
		},
		{
			name:     "decimal field scale too large",
			schema:   []Field{StringField{NameVal: "a"}, DecimalField{NameVal: "b", Scale: 20}},
			pkFields: []string{"a"},
			err:      "scale values outside the range",
		},
		{
			name:     "date int field",
			schema:   []Field{StringField{NameVal: "a"}, DateIntField{NameVal: "dif", Unit: Day}},
			pkFields: []string{"a"},
			rawRec:   []interface{}{"blah", time.Date(1972, time.January, 1, 1, 1, 1, 1, time.UTC)},
			rowID:    "blah",
			rowVals:  []interface{}{int64(730)},
		},
		{
			name:     "date int field string",
			schema:   []Field{StringField{NameVal: "a"}, DateIntField{NameVal: "dif", Unit: Day}},
			pkFields: []string{"a"},
			rawRec:   []interface{}{"blah", "1972-01-01"},
			rowID:    "blah",
			rowVals:  []interface{}{int64(730)},
		},
		{
			name:     "record time field custom units",
			schema:   []Field{StringField{NameVal: "b"}, RecordTimeField{NameVal: "recordTimeField1", Layout: "blah", Unit: Second}, StringField{NameVal: "domain", Quantum: "MDH"}},
			pkFields: []string{"b"},
			rawRec:   []interface{}{"blaah", 123654895, "molecula.com"},
			rowID:    "blaah",
			rowVals:  []interface{}{"molecula.com"},
			time:     getQuantizedTime(time.Unix(123654895, 0)),
		},
		{
			name:     "record time field layout",
			schema:   []Field{StringField{NameVal: "b"}, RecordTimeField{NameVal: "recordTimeField1", Layout: time.RFC822}, StringField{NameVal: "domain", Quantum: "MDH"}},
			pkFields: []string{"b"},
			rawRec:   []interface{}{"blaah", "08 Mar 09 21:00 UTC", "molecula.com"},
			rowID:    "blaah",
			rowVals:  []interface{}{"molecula.com"},
			time:     getQuantizedTime(time.Unix(1236548940, 0).UTC()),
		},
		{
			name:     "record time field epoch",
			schema:   []Field{StringField{NameVal: "b"}, RecordTimeField{NameVal: "recordTimeField1", Epoch: time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC), Unit: Second}, StringField{NameVal: "domain", Quantum: "MDH"}},
			pkFields: []string{"b"},
			rawRec:   []interface{}{"blaah", 1, "molecula.com"},
			rowID:    "blaah",
			rowVals:  []interface{}{"molecula.com"},
			time:     getQuantizedTime(time.Date(2010, time.January, 1, 0, 0, 1, 0, time.UTC)),
		},
		{
			name:      "bool parsing",
			schema:    []Field{BoolField{NameVal: "a"}, IDField{NameVal: "b"}, BoolField{NameVal: "c"}, BoolField{NameVal: "d"}, BoolField{NameVal: "e"}, BoolField{NameVal: "f"}},
			IDField:   "b",
			packBools: "bff",
			rawRec:    []interface{}{1, uint64(7), "1", "0", nil, '1'},
			rowID:     uint64(7),
			rowVals:   []interface{}{"a", "a", "c", "c", nil, "d", nil, nil, "f", "f"},
		},
		{
			name:     "timestamp field",
			schema:   []Field{StringField{NameVal: "a"}, TimestampField{NameVal: "b", Granularity: "s", Unit: Millisecond, Epoch: time.Unix(10000, 0)}},
			pkFields: []string{"a"},
			rawRec:   []interface{}{"blah", "5000"},
			rowID:    "blah",
			rowVals:  []interface{}{int64(5)},
		},
		{
			name:     "timestamp incorrect layout",
			schema:   []Field{StringField{NameVal: "a"}, TimestampField{NameVal: "b", Layout: "Mon, 02 Jan 2006 15:04:05 MST"}},
			pkFields: []string{"a"},
			rawRec:   []interface{}{"blah", "2025-05-15T05:05:05Z"},
			rowID:    "blah",
			rdzErrs:  []string{"", "parsing time string 2025-05-15T05:05:05Z"},
		},
		{
			name:     "timestamp empty string",
			schema:   []Field{StringField{NameVal: "a"}, TimestampField{NameVal: "b", Epoch: time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC), Unit: Nanosecond}},
			pkFields: []string{"a"},
			rawRec:   []interface{}{"blah", ""},
			rowID:    "blah",
			rowVals:  []interface{}{nil},
		},
		{
			name:     "timestamp nil input",
			schema:   []Field{StringField{NameVal: "a"}, TimestampField{NameVal: "b", Epoch: time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC), Unit: Nanosecond}},
			pkFields: []string{"a"},
			rawRec:   []interface{}{"blah", nil},
			rowID:    "blah",
			rowVals:  []interface{}{nil},
		},
		{
			name:     "timestamp epoch default granularity",
			schema:   []Field{StringField{NameVal: "a"}, TimestampField{NameVal: "b", Epoch: time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC), Unit: Nanosecond}},
			pkFields: []string{"a"},
			rawRec:   []interface{}{"blah", "5000000000"},
			rowID:    "blah",
			rowVals:  []interface{}{int64(5)},
		},
		{
			name:    "timestamp epoch default granularity with extgen",
			schema:  []Field{StringField{NameVal: "a"}, TimestampField{NameVal: "b", Epoch: time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC), Unit: Nanosecond}},
			autogen: true,
			extgen:  true,
			rawRec:  []interface{}{"blah", "5000000000"},
			rowVals: []interface{}{"blah", int64(5)},
		},
		{
			name:            "lookup text",
			schema:          []Field{StringField{NameVal: "a"}, DateIntField{NameVal: "dif", Unit: Day}, LookupTextField{NameVal: "log"}},
			pkFields:        []string{"a"},
			rawRec:          []interface{}{"blah", "1972-01-01", "asdf"},
			rowID:           "blah",
			rowVals:         []interface{}{int64(730)},
			lookupWriteIdxs: []int{2},
		},
		{
			name:     "int null",
			pkFields: []string{"user_id"},
			rawRec:   []interface{}{"1a", nil},
			rowID:    "1a",
			schema: []Field{StringField{NameVal: "user_id"},
				IntField{NameVal: "int_val"},
			},
			rowVals: []interface{}{nil},
		},
	}

	servers := []serverInfo{
		{
			Name:        "WithAuth",
			AuthToken:   getAuthToken(t),
			PilosaHosts: []string{"http://pilosa-auth:10105"},
		},
		{
			Name:        "WithOutAuth",
			PilosaHosts: []string{"http://pilosa:10101"},
		},
	}

	for _, server := range servers {
		for _, test := range tests {
			// test on fresh Pilosa
			t.Run(test.name+"-1-"+server.Name, func(t *testing.T) {
				runTest(t, test, false, server)
			})
			// test again with index/fields in place
			t.Run(test.name+"-2-"+server.Name, func(t *testing.T) {
				runTest(t, test, true, server)
			})
		}
	}

}

// When ingesting into a field that already exists in Pilosa,
// ensure that we check the compatibility of the Pilosa field
// with the source field.
func TestCheckFieldCompatibility(t *testing.T) {
	ingester := NewMain()
	configureTestFlags(ingester)

	// Get the pilosa client from ingester.
	if _, err := ingester.setupClient(); err != nil {
		t.Fatal(err)
	}
	client := ingester.PilosaClient()

	// Create various fields which we can use to check
	// ingest field compatibility.
	rand.Seed(time.Now().UTC().UnixNano())
	idxName := fmt.Sprintf("compattest%d", rand.Intn(100000))
	idx := pilosaclient.NewIndex(idxName)
	if err := client.CreateIndex(idx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ingester.PilosaClient().DeleteIndexByName(idxName); err != nil {
			t.Logf("%s for index %s: %v", idktest.ErrDeletingIndex, idxName, err)
		}
	}()
	if err := client.CreateField(idx.Field("pset", pilosaclient.OptFieldTypeSet(pilosaclient.CacheTypeNone, 0))); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateField(idx.Field("pint", pilosaclient.OptFieldTypeInt(-1000, 1000))); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateField(idx.Field("pttl", pilosaclient.OptFieldKeys(true), pilosaclient.OptFieldTypeTime("YMD"), pilosaclient.OptFieldTTL(0))); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateField(idx.Field(
		"packbools-exists",
		pilosaclient.OptFieldTypeSet(pilosaclient.CacheTypeRanked, pilosaclient.CacheSizeDefault),
		pilosaclient.OptFieldKeys(false),
	)); err != nil {
		t.Fatal(err)
	}

	ingester.AutoGenerate = true
	ingester.Index = idxName
	ingester.index = idx
	ingester.BatchSize = 5
	ingester.PackBools = "packbools"

	tests := []struct {
		fld    Field
		val    interface{}
		expErr string
	}{
		{
			fld:    StringField{NameVal: "pset"},
			val:    "a",
			expErr: "idk keys true differs from featurebase keys false",
		},
		{
			fld:    IntField{NameVal: "pset"},
			val:    1,
			expErr: "idk field type int is incompatible with featurebase field type set",
		},
		{
			fld:    IntField{NameVal: "pint"},
			val:    1,
			expErr: "",
		},
		{
			fld:    IntField{NameVal: "pint", Min: int64Ptr(-100)},
			val:    1,
			expErr: "idk min -100 differs from featurebase min -1000",
		},
		{
			fld:    IntField{NameVal: "pint", Min: int64Ptr(-1000), Max: int64Ptr(100)},
			val:    1,
			expErr: "idk max 100 differs from featurebase max 1000",
		},
		{
			fld:    BoolField{NameVal: "foo"},
			val:    true,
			expErr: "idk keys true differs from featurebase keys false: packbools-exists",
		},
		{
			fld:    StringField{NameVal: "pttl", Quantum: "YMD1", TTL: "1s"},
			val:    "a",
			expErr: "",
		},
		{
			fld:    StringField{NameVal: "pttl", Quantum: "YMD1", TTL: "bad-format"},
			val:    "a",
			expErr: "",
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			ts := newTestSource(
				[]Field{test.fld},
				[][]interface{}{
					{test.val},
				},
			)
			ingester.NewSource = func() (Source, error) { return ts, nil }
			err := ingester.Run()
			if test.expErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.expErr) {
					t.Fatalf("expected error: '%s', but got: '%v'", test.expErr, err)
				}
			}
		})
	}
}

func int64Ptr(i int64) *int64 {
	return &i
}

func testErr(t *testing.T, exp string, actual error) (done bool) {
	t.Helper()
	if exp == "" && actual == nil {
		return false
	}
	if exp == "" && actual != nil {
		t.Fatalf("unexpected errs exp: \n%s got: \n%v", exp, actual)
	}
	if exp != "" && actual == nil {
		t.Fatalf("expected errs exp: \n%s got: \n%v", exp, actual)
	}
	if !strings.Contains(actual.Error(), exp) {
		t.Fatalf("unmatched errs exp: \n%s got: \n%v", exp, actual)
	}
	return true
}

type signalingTestSource struct {
	records   chan []interface{}
	schema    []Field
	committed chan []interface{}
}

func (s *signalingTestSource) Record() (Record, error) {
	rec, ok := <-s.records
	if !ok {
		return nil, io.EOF
	}
	return newSliceRecord(rec, s.committed), nil
}

func (s *signalingTestSource) Schema() []Field {
	return s.schema
}
func (s *signalingTestSource) Close() error {
	return nil
}

func newSignalingTestSource(schema []Field, records chan []interface{}, committed chan []interface{}) *signalingTestSource {
	return &signalingTestSource{
		schema:    schema,
		records:   records,
		committed: committed,
	}
}

type testSource struct {
	i       int
	records [][]interface{}
	schema  []Field
}

func (t *testSource) Close() error {
	return nil
}

type sliceRecord struct {
	data      []interface{}
	committed chan []interface{}
}

func (s *sliceRecord) Commit(ctx context.Context) error {
	if s.committed == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.committed <- s.data:
		return nil
	}
}

func (s *sliceRecord) Data() []interface{} {
	return s.data
}

func (s *testSource) Record() (Record, error) {
	s.i++
	if s.i <= len(s.records) {
		if s.records[s.i-1][0] == ErrFlush {
			return nil, ErrFlush
		}
		return newSliceRecord(s.records[s.i-1], nil), nil
	}
	return nil, io.EOF
}

func newSliceRecord(data []interface{}, committed chan []interface{}) *sliceRecord {
	return &sliceRecord{
		data:      data,
		committed: committed,
	}
}

func (s *testSource) Schema() []Field {
	return s.schema
}

func newTestSource(schema []Field, records [][]interface{}) *testSource {
	return &testSource{
		schema:  schema,
		records: records,
	}
}

func stringSliceSame(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		aa, bb := a[i], b[i]
		if aa != bb {
			return false
		}
	}
	return true
}

// generate authentication token for testing with pilosa-auth
// featurebase.conf used for running pilosa-auth contains same auth parameters
func getAuthToken(t *testing.T) string {
	t.Helper()
	var (
		ClientID         = "e9088663-eb08-41d7-8f65-efb5f54bbb71"
		ClientSecret     = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
		AuthorizeURL     = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/authorize"
		TokenURL         = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/token"
		GroupEndpointURL = "https://graph.microsoft.com/v1.0/me/transitiveMemberOf/microsoft.graph.group?$count=true"
		LogoutURL        = "https://login.microsoftonline.com/common/oauth2/v2.0/logout"
		Scopes           = []string{"https://graph.microsoft.com/.default", "offline_access"}
		Key              = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
		ConfiguredIps    = []string{}
	)

	a, err := authn.NewAuth(
		logger.NewStandardLogger(os.Stdout),
		"http://localhost:10101/",
		Scopes,
		AuthorizeURL,
		TokenURL,
		GroupEndpointURL,
		LogoutURL,
		ClientID,
		ClientSecret,
		Key,
		ConfiguredIps,
	)
	if err != nil {
		t.Fatal(err)
	}

	// make a valid token
	tkn := jwt.New(jwt.SigningMethodHS256)
	claims := tkn.Claims.(jwt.MapClaims)
	claims["oid"] = "42"
	claims["name"] = "valid"
	token, err1 := tkn.SignedString([]byte(a.SecretKey()))
	if err1 != nil {
		t.Fatal(err1)
	}

	return token
}

func TestSetup(t *testing.T) {
	m := NewMain()
	configureTestFlags(m)
	m.AutoGenerate = true
	m.Index = "test"
	m.NewSource = func() (Source, error) { return nil, nil }

	token := getAuthToken(t)
	m.AuthToken = token

	t.Run("test", func(t *testing.T) {

		_, err := m.Setup()
		if err != nil {
			t.Fatalf("Failed idk main setup: %s", err)
		}
		got := m.AuthToken
		want := "Bearer " + token
		if got != want {
			t.Fatalf("got %v, but want %v", got, want)
		}
	})
}
func TestNilIngest(t *testing.T) {
	type testcase struct {
		name       string
		schema     []Field
		IDField    string
		pkFields   []string
		rawRec1    []interface{}
		rawRec2    []interface{}
		rawRec3    []interface{}
		rowID      interface{}
		clearVals1 map[int]interface{}
		clearVals2 map[int]interface{}
		clearVals3 map[int]interface{}
		Vals1      []interface{}
		Vals2      []interface{}
		Vals3      []interface{}
		err        string
		batchErr   string
		rdzErrs    []string
	}
	runTest := func(t *testing.T, test testcase, removeIndex bool, server serverInfo, rawRec []interface{}, clearmap map[int]interface{}, values []interface{}) {
		m := NewMain()
		configureTestFlags(m)
		m.Index = "cmd_test_index23l"
		m.PrimaryKeyFields = test.pkFields
		m.BatchSize = 2
		m.Pprof = ""
		m.NewSource = func() (Source, error) { return nil, nil }
		if server.AuthToken != "" {
			m.AuthToken = server.AuthToken
		}
		m.PilosaHosts = server.PilosaHosts
		onFinishRun, err := m.Setup()
		if strings.Contains("validation", test.name) && testErr(t, test.err, err) {
			return
		}
		if err != nil {
			t.Fatalf("%v", err)
		}
		defer onFinishRun()
		if removeIndex {
			defer func() {
				err := m.client.DeleteIndex(m.index)
				if err != nil {
					t.Logf("%s for index %s: %v", idktest.ErrDeletingIndex, m.Index, err)
				}
			}()
		}

		rdzs, batch, row, _, err := m.batchFromSchema(test.schema)
		if testErr(t, test.err, err) {
			return
		}
		for k, rdz := range rdzs {
			err = rdz(rawRec, row)
			if test.rdzErrs != nil && test.rdzErrs[k] != "" && testErr(t, test.rdzErrs[k], err) {
				return
			}
		}

		if !reflect.DeepEqual(row.Clears, clearmap) {
			t.Fatalf("clear exp: %+v got %+v", clearmap, row.Clears)
		}
		if !reflect.DeepEqual(row.Values, values) {
			t.Fatalf("values exp: %+v got %+v", values, row.Values)
		}
		err = batch.Add(*row)
		if testErr(t, test.batchErr, err) {
			return
		}
	}

	tests := []testcase{
		{
			name:     "int/dec null",
			pkFields: []string{"user_id"},
			rawRec1:  []interface{}{"1a", nil, 5, nil, 5.2},
			rawRec2:  []interface{}{"1a", 2, nil, 4.4, DELETE_SENTINEL},
			rawRec3:  []interface{}{"1a", DELETE_SENTINEL, 5, nil, 5.2},
			rowID:    "1a",
			schema: []Field{
				StringField{NameVal: "user_id"},
				IntField{NameVal: "int_val1"},
				IntField{NameVal: "int_val2"},
				DecimalField{NameVal: "dec_val1", Scale: 1},
				DecimalField{NameVal: "dec_val2", Scale: 1},
			},
			clearVals1: map[int]interface{}{},
			clearVals2: map[int]interface{}{3: uint64(0)},
			clearVals3: map[int]interface{}{0: uint64(0)},
			Vals1:      []interface{}{nil, int64(5), nil, int64(52)},
			Vals2:      []interface{}{int64(2), nil, int64(44), nil},
			Vals3:      []interface{}{nil, int64(5), nil, int64(52)},
		}, {
			name:     "mutex null",
			pkFields: []string{"user_id"},
			rawRec1:  []interface{}{"1a", "g", uint64(1)},
			rawRec2:  []interface{}{"1a", DELETE_SENTINEL, nil},
			rawRec3:  []interface{}{"1a", nil, DELETE_SENTINEL},
			rowID:    "1a",
			schema: []Field{
				StringField{NameVal: "user_id"},
				StringField{NameVal: "string_val1", Mutex: true},
				IDField{NameVal: "id_val1", Mutex: true},
			},
			clearVals1: map[int]interface{}{},
			clearVals2: map[int]interface{}{0: nil},
			clearVals3: map[int]interface{}{1: nil},
			Vals1:      []interface{}{"g", uint64(1)},
			Vals2:      []interface{}{nil, nil},
			Vals3:      []interface{}{nil, nil},
		}, {
			name:     "bools null",
			pkFields: []string{"user_id"},
			rawRec1:  []interface{}{"1a", true}, // bool and bool-exists
			rawRec2:  []interface{}{"1a", DELETE_SENTINEL},
			rawRec3:  []interface{}{"1a", nil},
			rowID:    "1a",
			schema: []Field{
				StringField{NameVal: "user_id"},
				BoolField{NameVal: "bool_val_1"},
			},
			clearVals1: map[int]interface{}{},
			clearVals2: map[int]interface{}{0: "bool_val_1"},
			clearVals3: map[int]interface{}{},
			Vals1:      []interface{}{"bool_val_1", "bool_val_1"},
			Vals2:      []interface{}{nil, nil},
			Vals3:      []interface{}{nil, nil},
		}, {
			name:     "set no-op",
			pkFields: []string{"user_id"},
			rawRec1:  []interface{}{"1a", uint64(1)},
			rawRec2:  []interface{}{"1a", DELETE_SENTINEL},
			rawRec3:  []interface{}{"1a", nil},
			rowID:    "1a",
			schema: []Field{
				StringField{NameVal: "user_id"},
				IDField{NameVal: "id_val1"},
			},
			clearVals1: map[int]interface{}{},
			clearVals2: map[int]interface{}{},
			clearVals3: map[int]interface{}{},
			Vals1:      []interface{}{uint64(1)},
			Vals2:      []interface{}{nil},
			Vals3:      []interface{}{nil},
		},
	}

	servers := []serverInfo{
		{
			Name:        "WithAuth",
			AuthToken:   getAuthToken(t),
			PilosaHosts: []string{"http://pilosa-auth:10105"},
		},
		{
			Name:        "WithOutAuth",
			PilosaHosts: []string{"http://pilosa:10101"},
		},
	}

	for _, server := range servers {
		for _, test := range tests {
			// test on fresh Pilosa
			t.Run(test.name+"-1-"+server.Name, func(t *testing.T) {
				runTest(t, test, false, server, test.rawRec1, test.clearVals1, test.Vals1)
			})
			// test again with index/fields in place
			t.Run(test.name+"-2-"+server.Name, func(t *testing.T) {
				runTest(t, test, false, server, test.rawRec2, test.clearVals2, test.Vals2)
			})
			t.Run(test.name+"-3-"+server.Name, func(t *testing.T) {
				runTest(t, test, true, server, test.rawRec3, test.clearVals3, test.Vals3)
			})
		}
	}

}
