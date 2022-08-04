package csv

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/molecula/featurebase/v3/idk/idktest"
)

func configureTestFlags(main *Main) {
	if pilosaHost, ok := os.LookupEnv("IDK_TEST_PILOSA_HOST"); ok {
		main.PilosaHosts = []string{pilosaHost}
	} else {
		main.PilosaHosts = []string{"pilosa:10101"}
	}
	if grpcHost, ok := os.LookupEnv("IDK_TEST_GRPC_HOST"); ok {
		main.PilosaGRPCHosts = []string{grpcHost}
	} else {
		main.PilosaGRPCHosts = []string{"pilosa:20101"}
	}
	main.Stats = ""
}

func TestCSVCommand(t *testing.T) {
	file := `
id__ID,s__String_F_YMDH,__RecordTime_2006-01-02T15
0,a,2019-01-09T04
1,a,2019-01-09T05
2,a,2019-02-09T04
3,b,2019-01-09T04
4,a,2018-01-09T04
5,a,2019-01-10T04
`[1:]
	name := writeTempFile(t, file)

	m := NewMain()
	m.AutoGenerate = true
	configureTestFlags(m)
	m.Files = []string{name}
	rand.Seed(time.Now().UnixNano())
	m.Index = fmt.Sprintf("csvtest%d", rand.Intn(100000))

	defer func() {
		if err := m.PilosaClient().DeleteIndexByName(m.Index); err != nil {
			t.Logf("deleting test index: %v", err)
		}
	}()

	err := m.Run()
	if err != nil {
		t.Fatalf("running: %v", err)
	}

	client := m.PilosaClient()

	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	index := schema.Index(m.Index)

	s := index.Field("s")

	resp, err := client.Query(s.Range("a", tim(t, "2019-01-07T03"), tim(t, "2019-01-10T05")))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	if !reflect.DeepEqual(resp.Results()[0].Row().Columns, []uint64{0, 1, 5}) {
		t.Errorf("got/exp\n%v\n%v", resp.Results()[0].Row().Columns, []uint64{0, 1, 5})
	}
}

func TestCSVCommandCustomHeader(t *testing.T) {
	file := `
ABCD,2019-01-02,70%
ABCD,2019-01-03,20%
ABCD,2019-01-04,30%
BEDF,2019-01-02,70%
BEDF,2019-01-05,90%
BEDF,2019-01-08,10%
BEDF,2019-01-08,20%
ABCD,2019-01-30,40%
`[1:]
	name := writeTempFile(t, file)

	m := NewMain()
	m.AutoGenerate = true
	m.Header = []string{"asset_tag__String", "fan_time__RecordTime_2006-01-02"}
	configureTestFlags(m)
	m.Files = []string{name}
	rand.Seed(time.Now().UnixNano())
	m.Index = fmt.Sprintf("csvtest%d", rand.Intn(100000))

	defer func() {
		if err := m.PilosaClient().DeleteIndexByName(m.Index); err != nil {
			t.Logf("deleting test index: %v", err)
		}
	}()

	err := m.Run()
	if err != nil {
		t.Fatalf("running: %v", err)
	}

	client := m.PilosaClient()

	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	index := schema.Index(m.Index)

	s := index.Field("asset_tag")

	resp, err := client.Query(s.Row("ABCD"))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	if !reflect.DeepEqual(resp.Results()[0].Row().Columns, []uint64{0, 1, 2, 7}) {
		t.Errorf("got/exp\n%v\n%v", resp.Results()[0].Row().Columns, []uint64{0, 1, 2, 7})
	}
}

type recordTimeTestCase struct {
	name   string
	layout string
	unit   string
	scale  int // how many unit it takes to move an hour
}

func TestCSVRecordTime(t *testing.T) {
	testCases := []recordTimeTestCase{
		{name: "plusHours", layout: "", unit: "h", scale: 1},
		{name: "plusMinutes", layout: "", unit: "m", scale: 60},
		{name: "plusSeconds", layout: "", unit: "s", scale: 60 * 60},
	}
	// We set our epoch at 4AM, 2019-01-09, because that's the epoch used in
	// the previous example.
	arbitraryEpoch, err := time.Parse("2006-01-02T15", "2019-01-09T04")
	if err != nil {
		t.Fatalf("parsing time: %v", err)
	}

	for _, tc := range testCases {
		lines := []string{}
		layout := tc.layout
		if layout == "" {
			layout = time.RFC3339
		}
		header := fmt.Sprintf(`id__ID,s__String_F_YMDH,__RecordTime_%s_%s_%s`,
			layout, arbitraryEpoch.Format(layout), tc.unit)
		lines = append(lines, header)
		// We expect -1..1 to show up in our query, and -2 and +2 not to.
		for i, offset := range []int{-2, -1, 0, 1, 2} {
			record := fmt.Sprintf("%d,a,%d", i, offset*tc.scale)
			lines = append(lines, record)
		}
		lines = append(lines, "")
		file := strings.Join(lines, "\n")

		name := writeTempFile(t, file)

		m := NewMain()
		m.AutoGenerate = true
		configureTestFlags(m)
		m.Files = []string{name}
		rand.Seed(time.Now().UnixNano())
		m.Index = fmt.Sprintf("csvtest%d", rand.Intn(100000))

		defer func() {
			if err := m.PilosaClient().DeleteIndexByName(m.Index); err != nil {
				t.Logf("deleting test index: %v", err)
			}
		}()

		err := m.Run()
		if err != nil {
			t.Fatalf("running: %v", err)
		}

		client := m.PilosaClient()

		schema, err := client.Schema()
		if err != nil {
			t.Fatalf("getting schema: %v", err)
		}
		index := schema.Index(m.Index)

		s := index.Field("s")

		// We specify a range of one minute after the hour because otherwise
		// we hit the end of the range and don't count the +1 case. I am
		// not convinced that this is correct.
		resp, err := client.Query(s.Range("a", tim(t, "2019-01-09T03"), tim(t, "2019-01-09T05").Add(1*time.Minute)))
		if err != nil {
			t.Fatalf("querying: %v", err)
		}

		if !reflect.DeepEqual(resp.Results()[0].Row().Columns, []uint64{1, 2, 3}) {
			t.Errorf("record time (%s): got/exp\n%v\n%v", tc.name, resp.Results()[0].Row().Columns, []uint64{1, 2, 3})
		}
	}
}

func tim(t *testing.T, tstr string) time.Time {
	ti, err := time.Parse("2006-01-02T15", tstr)
	if err != nil {
		t.Fatalf("parsing time: %v", err)
	}
	return ti
}

func writeTempFile(t *testing.T, data string) string {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("getting temp file: %v", err)
	}
	defer f.Close()

	_, err = io.WriteString(f, data)
	if err != nil {
		t.Fatalf("writing string: %v", err)
	}

	return f.Name()
}

func TestStreamFileNames(t *testing.T) {
	tmp, err := testDirTree("d1/d2/d3/d4/d5")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	mtx := &sync.Mutex{}
	visited := make(map[string]int)

	m := NewMain()
	configureTestFlags(m)
	// duplicate all files from the same directory
	m.Files = []string{tmp, tmp, tmp, tmp, tmp, tmp}
	_, err = m.NewSource()
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	visitor := func(files chan string) {
		defer wg.Done()

		for f := range files {
			// count all visited files
			mtx.Lock()
			visited[f]++
			mtx.Unlock()
		}
	}

	wg.Add(3)
	go visitor(m.files)
	go visitor(m.files)
	go visitor(m.files)
	wg.Wait()

	for p, n := range visited {
		fi, err := os.Lstat(p)
		if err != nil {
			t.Fatal(err)
		}

		if fi.Mode().IsRegular() && n != 1 {
			t.Fatalf("file %s was visited %d times", p, n)
		}
	}
}

func testDirTree(tree string) (string, error) {
	tmp, err := ioutil.TempDir("", "")
	if err != nil {
		return "", err
	}

	err = os.MkdirAll(filepath.Join(tmp, tree), 0755)
	if err != nil {
		os.RemoveAll(tmp)
		return "", err
	}

	return tmp, filepath.Walk(tmp, func(path string, _ os.FileInfo, _ error) error {
		f, err := os.Create(filepath.Join(path, "csv"))
		if err != nil {
			return err
		}
		return f.Close()
	})
}

// Tests various out of range values are ingested as nil when CLI flags are set to true.
func TestVariousOORValues(t *testing.T) {
	file := `
id__ID,s__String_F_YMDH,ts__Timestamp_s_2006-01-02 15:04:05.999,price__Decimal_2,age__Int_1_120
0,a,0000-01-03 08:00:00.000,0.0,1
1,b,9999-12-31 23:59:60.999,5.44,35
2,b,2019-50-03 08:00:00.000,5.44,120
3,b,2019-01-50 08:00:00.000,5.44,120
4,b,2019-01-03 50:00:00.000,5.44,120
5,a,2019-04-03 00:90:00.000,5.44,129
6,a,2019-04-03 00:00:90.000,5.44,120
5,a,2019-04-03 00:00:00.000,123.123,1
6,a,2019-04-03 00:00:00.000,-1,1
7,a,2019-04-03 00:00:00.000,994492233720368547758.0892233720368547758,100
5,a,2019-04-03 00:00:00.000,2.34,121
6,a,2019-04-03 00:00:00.000,3.44,0
7,a,1500-04-03 00:00:00.000,994492233720368547758.0892233720368547758,2342342
8,a,2019-04-03 00:00:00.000,3.44,100
`[1:]
	name := writeTempFile(t, file)

	checker := make(map[string]interface{})
	checker["ts"] = []interface{}{nil, nil, nil, nil, nil, nil, nil, "2019-04-03T00:00:00Z", "2019-04-03T00:00:00Z", "2019-04-03T00:00:00Z", "2019-04-03T00:00:00Z", "2019-04-03T00:00:00Z", "1500-04-03T00:00:00Z", "2019-04-03T00:00:00Z"}
	checker["age"] = []interface{}{1, 35, 120, 120, 120, nil, 120, 1, 1, 100, nil, nil, nil, 100}
	checker["price"] = []interface{}{0.0, 5.44, 5.44, 5.44, 5.44, 5.44, 5.44, 123.12, -1.0, nil, 2.34, 3.44, nil, 3.44}

	m := NewMain()
	m.AutoGenerate = true
	m.BatchSize = 1
	configureTestFlags(m)
	m.Files = []string{name}
	rand.Seed(time.Now().UnixNano())
	m.Index = fmt.Sprintf("csvtest%d", rand.Intn(100000))
	m.AllowIntOutOfRange = true
	m.AllowDecimalOutOfRange = true
	m.AllowTimestampOutOfRange = true

	defer func() {
		if err := m.PilosaClient().DeleteIndexByName(m.Index); err != nil {
			t.Logf("deleting test index: %v", err)
		}
	}()

	err := m.Run()
	if err != nil {
		t.Fatalf("running: %v", err)
	}

	client := m.PilosaClient()

	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	index := schema.Index(m.Index)

	resp, err := client.Query(index.Count(index.All()))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if cnt := resp.Result().Count(); cnt != 14 {
		t.Fatalf("expected 14, got %+v", cnt)
	}

	for _, field := range []string{"ts", "age", "price"} {
		pql := fmt.Sprintf("Extract(All(), Rows(%s))", field)

		eResp, err := idktest.DoExtractQuery(pql, m.Index)
		if err != nil {
			t.Fatal("doing extract: ", err)
		}
		if eResp.Results[0].Columns == nil {
			t.Fatal("no results: ", err)
		}

		for i, item := range eResp.Results[0].Columns {
			check := checker[field].([]interface{})

			switch exp := check[i].(type) {
			case nil:
				if item.Rows[0] != nil {
					t.Errorf("expected nil, got %+v", item.Rows[0])
				}
			case string:
				if item.Rows[0] != exp {
					t.Errorf("expected %s, got %+v", exp, item.Rows[0])
				}
			case int:
				vAsInt := int(item.Rows[0].(float64))
				if vAsInt != exp {
					t.Errorf("expected %d, got %+v", exp, item.Rows[0])
				} else {
					expAsFloat := float64(exp)
					if item.Rows[0] != expAsFloat {
						t.Errorf("expected %f, got %+v", expAsFloat, item.Rows[0])
					}
				}
			case float64:
				if item.Rows[0] != exp {
					t.Errorf("expected %f, got %+v", exp, item.Rows[0])
				}
			default:
				t.Errorf("unknown type: %T", exp)
			}
		}
	}

}

type TimestampTestCase struct {
	fieldName string
	file      string
	expect    interface{}
}

// Test Time Layouts
func TestTimeLayouts(t *testing.T) {
	testCases := []TimestampTestCase{
		{
			fieldName: "ts1",
			file: `
id__ID,s__String_F_YMDH,ts1__Timestamp_s_2006-01-02 15:04:05.999_2030-01-02 15:04:05.999_s
0,a,99221100

`[1:],
			expect: []interface{}{"2033-02-24T00:29:05Z"},
		},
		{
			fieldName: "ts-nano-min",
			file: `
id__ID,s__String_F_YMDH,ts-nano-min__Timestamp_ns_2006-01-02T15:04:05.999999999Z_1833-11-24T17:31:44.01Z_s
0,a,1
1,b,-1
`[1:],
			expect: []interface{}{"1833-11-24T17:31:45.01Z", nil},
		},
		{
			fieldName: "ts-nano-max",
			file: `
id__ID,s__String_F_YMDH,ts-nano-max__Timestamp_ns_2006-01-02T15:04:05.999999999Z_2106-02-07T06:28:16Z_ns
0,a,1
1,b,-1000001
`[1:],
			expect: []interface{}{nil, "2106-02-07T06:28:15.998999999Z"},
		},
		{
			fieldName: "ts-sec-min",
			file: `
id__ID,s__String_F_YMDH,ts-sec-min__Timestamp_s_2006-01-02T15:04:05.999999999Z_0001-01-01T00:00:01Z_ms
0,a,1001
1,b,-1001
`[1:],
			expect: []interface{}{"0001-01-01T00:00:02Z", nil},
		},
		{
			fieldName: "ts-ms-max",
			file: `
id__ID,s__String_F_YMDH,ts-ms-max__Timestamp_ms_2006-01-02T15:04:05.999999999Z_9999-12-31T23:59:59Z_us
0,a,1001
1,b,-1001

`[1:],
			expect: []interface{}{nil, "9999-12-31T23:59:58.999Z"},
		},
		{
			fieldName: "gran-conversion",
			file: `
id__ID,s__String_F_YMDH,gran-conversion__Timestamp_ns_2006-01-02T15:04:05.999999999Z_2000-02-07T06:28:16Z_s
0,a,10000000000
1,b,-1001

`[1:],
			expect: []interface{}{nil, "2000-02-07T06:11:35Z"},
		},
	}

	for _, tc := range testCases {
		m := newMainOORFactory(t, tc.file, false, false, true)
		testTimestampRunner(t, m, tc)
	}
}

func testTimestampRunner(t *testing.T, m *Main, testCase TimestampTestCase) {
	defer func() {
		if err := m.PilosaClient().DeleteIndexByName(m.Index); err != nil {
			t.Logf("deleting test index: %v", err)
		}
	}()

	err := m.Run()
	if err != nil {
		t.Fatalf("running: %v", err)
	}

	pql := fmt.Sprintf("Extract(All(), Rows(%s))", testCase.fieldName)

	eResp, err := idktest.DoExtractQuery(pql, m.Index)
	if err != nil {
		t.Fatal("doing extract: ", err)
	}
	if eResp.Results[0].Columns == nil {
		t.Fatal("no results: ", err)
	}

	for i, item := range eResp.Results[0].Columns {
		check := testCase.expect.([]interface{})
		switch exp := check[i].(type) {
		case nil:
			if item.Rows[0] != nil {
				t.Errorf("expected nil, got %+v", item.Rows[0])
			}
		case string:
			if item.Rows[0] != exp {
				t.Errorf("expected %s, got %+v", exp, item.Rows[0])
			}
		case int:
			vAsInt := int(item.Rows[0].(float64))
			if vAsInt != exp {
				t.Errorf("expected %d, got %+v", exp, item.Rows[0])
			} else {
				expAsFloat := float64(exp)
				if item.Rows[0] != expAsFloat {
					t.Errorf("expected %f, got %+v", expAsFloat, item.Rows[0])
				}
			}
		case float64:
			if item.Rows[0] != exp {
				t.Errorf("expected %f, got %+v", exp, item.Rows[0])
			}
		default:
			t.Errorf("unknown type: %T", exp)
		}
	}

}

// Test that out of range int values are ingested as nil when AllowIntOutOfRange is true.
func TestIntOpts(t *testing.T) {
	file := `
id__ID,negneg__Int_-10_-5,negpos__Int_-10_10,pospos__Int_5_10,negzero__Int_-10_0,zeropos__Int_0_10,zerozero__Int_0_0
1,-20,-20,-20,-20,-20,-20
2,-10,-10,-10,-10,-10,-10
3,-5,-5,-5,-5,-5,-5
4,0,0,0,0,0,0
5,5,5,5,5,5,5
6,10,10,10,10,10,10
7,20,20,20,20,20,20
`[1:]

	checker := make(map[string]interface{})
	checker["negneg"] = []interface{}{nil, -10, -5, nil, nil, nil, nil}
	checker["negzero"] = []interface{}{nil, -10, -5, 0, nil, nil, nil}
	checker["negpos"] = []interface{}{nil, -10, -5, 0, 5, 10, nil}
	checker["zeropos"] = []interface{}{nil, nil, nil, 0, 5, 10, nil}
	checker["zerozero"] = []interface{}{nil, nil, nil, 0, nil, nil, nil}
	checker["pospos"] = []interface{}{nil, nil, nil, nil, 5, 10, nil}

	batchSizes := []int{1, 3, 4, 10}
	for _, bsize := range batchSizes {
		t.Run(fmt.Sprintf("batchsize=%d", bsize), func(t *testing.T) {
			m := newMainOORFactory(t, file, true, false, false)
			m.BatchSize = bsize

			defer func() {
				if err := m.PilosaClient().DeleteIndexByName(m.Index); err != nil {
					t.Logf("deleting test index: %v", err)
				}
			}()

			err := m.Run()
			if err != nil {
				t.Fatalf("running: %v", err)
			}

			for _, field := range []string{"negneg", "negzero", "negpos", "zeropos", "pospos"} {
				pql := fmt.Sprintf("Extract(All(), Rows(%s))", field)

				eResp, err := idktest.DoExtractQuery(pql, m.Index)
				if err != nil {
					t.Fatal("doing extract: ", err)
				}
				if eResp.Results[0].Columns == nil {
					t.Fatal("no results: ", err)
				}

				for i, item := range eResp.Results[0].Columns {
					check := checker[field].([]interface{})
					switch exp := check[i].(type) {
					case nil:
						if item.Rows[0] != nil {
							t.Errorf("expected nil, got %+v", item.Rows[0])
						}
					case string:
						if item.Rows[0] != exp {
							t.Errorf("expected %s, got %+v", exp, item.Rows[0])
						}
					case int:
						vAsInt := int(item.Rows[0].(float64))
						if vAsInt != exp {
							t.Errorf("expected %d, got %+v", exp, item.Rows[0])
						} else {
							expAsFloat := float64(exp)
							if item.Rows[0] != expAsFloat {
								t.Errorf("expected %f, got %+v", expAsFloat, item.Rows[0])
							}
						}
					case float64:
						if item.Rows[0] != exp {
							t.Errorf("expected %f, got %+v", exp, item.Rows[0])
						}
					default:
						t.Errorf("unknown type: %T", exp)
					}
				}
			}
		})
	}

}

// Test that out of range timestamp values are ingested as nil when AllowTimestampOutOfRange is true.
func TestTimestampOOR(t *testing.T) {
	file := `
id__ID,ts1__Timestamp_ns_2006-01-02 15:04:05.999,ts2__Timestamp_s_2006-01-02T15:04:05Z07:00_9998-12-31T15:04:05Z_h,ts3__Timestamp_s_2006-01-02T15:04:05Z07:00_0002-12-31T15:04:05Z_h,ts4__Timestamp_s_2006-01-02T15:04:05.999Z
0,1833-01-03 08:00:00.000,8500,8500,0001-01-01T00:00:00Z
1,1833-11-24 17:31:44.000,8769,-8500,0001-01-01T00:00:01Z
2,1833-11-25 17:31:44.000,-99991,0,0001-01-01T00:00:02Z
3,2106-02-06 06:28:16.000,0,-99995,9999-12-31T23:59:58Z
4,2106-02-07 06:28:16.000,9999,-99999,9999-12-31T23:59:59Z
5,2106-02-08 06:28:16.000,99999999999999999999999,-9999999999999999999999999,9999-12-31T23:59:60Z
`[1:]

	checker := make(map[string]interface{})
	// should import nil if timestamp val is out of range
	checker["ts1"] = []interface{}{nil, "1833-11-24T17:31:44Z", "1833-11-25T17:31:44Z", "2106-02-06T06:28:16Z", "2106-02-07T06:28:16Z", nil}
	// should import nil if custom epoch + value overflows
	checker["ts2"] = []interface{}{"9999-12-20T19:04:05Z", nil, "9987-08-05T08:04:05Z", "9998-12-31T15:04:05Z", nil, nil}
	checker["ts3"] = []interface{}{"0003-12-20T19:04:05Z", "0002-01-11T11:04:05Z", "0002-12-31T15:04:05Z", nil, nil, nil}
	checker["ts4"] = []interface{}{nil, "0001-01-01T00:00:01Z", "0001-01-01T00:00:02Z", "9999-12-31T23:59:58Z", "9999-12-31T23:59:59Z", nil}
	batchSizes := []int{3, 1, 4, 10}
	for _, bsize := range batchSizes {

		t.Run(fmt.Sprintf("batchsize=%d", bsize), func(t *testing.T) {
			m := newMainOORFactory(t, file, false, false, true)
			m.BatchSize = bsize

			defer func() {
				if err := m.PilosaClient().DeleteIndexByName(m.Index); err != nil {
					t.Logf("deleting test index: %v", err)
				}
			}()

			err := m.Run()
			if err != nil {
				t.Fatalf("running: %v", err)
			}
			for _, field := range []string{"ts1", "ts2", "ts3", "ts4"} {
				pql := fmt.Sprintf("Extract(All(), Rows(%s))", field)

				eResp, err := idktest.DoExtractQuery(pql, m.Index)
				if err != nil {
					t.Fatal("doing extract: ", err)
				}
				if eResp.Results[0].Columns == nil {
					t.Fatal("no results: ", err)
				}

				for i, item := range eResp.Results[0].Columns {
					check := checker[field].([]interface{})

					switch exp := check[i].(type) {
					case nil:
						if item.Rows[0] != nil {
							t.Errorf("expected nil, got %+v", item.Rows[0])
						}
					case string:
						if item.Rows[0] != exp {
							t.Errorf("expected %s, got %+v", exp, item.Rows[0])
						}
					default:
						t.Errorf("unknown type: %T", exp)
					}
				}
			}
		})
	}

}

// Tests various conditions that should halt ingest
func TestFailureConditions(t *testing.T) {
	type testCase struct {
		name                string
		csv                 string
		fail                bool
		intOutOfRange       bool
		decimalOutOfRange   bool
		timestampOutOfRange bool
	}

	testCases := []testCase{
		{name: "too small", csv: `id__ID,ts1__Timestamp_s_2006-01-02T15:04:05Z07:00_0000-01-01T00:00:00Z_h
0,0
`, fail: true, intOutOfRange: true, timestampOutOfRange: true, decimalOutOfRange: true},
		{name: "just right", csv: `id__ID,ts1__Timestamp_s_2006-01-02T15:04:05Z07:00_2200-12-31T15:04:05Z_h
0,0
`, fail: false, intOutOfRange: true, timestampOutOfRange: true, decimalOutOfRange: true},
		{name: "too big", csv: `id__ID,ts1__Timestamp_s_2006-01-02T15:04:05Z07:00_9999-12-31T23:59:60Z_h
0,0
`, fail: true, intOutOfRange: true, timestampOutOfRange: true, decimalOutOfRange: true},
		{name: "intOutOfRange not allowed", csv: `id__ID,pospos__Int_5_10
0,4
`, fail: true, intOutOfRange: false, timestampOutOfRange: true, decimalOutOfRange: true},
		{name: "intOutOfRange not allowed-2", csv: `id__ID,pospos__Int_5_10
0,11
`, fail: true, intOutOfRange: false, timestampOutOfRange: true, decimalOutOfRange: true},
		{name: "timestampOutOfRange not allowed", csv: `id__ID,ts1__Timestamp_s_2006-01-02 15:04:05.999
0,-0001-01-03 08:00:00.000
`, fail: true, intOutOfRange: true, timestampOutOfRange: false, decimalOutOfRange: true},
		{name: "timestampOutOfRange not allowed-2", csv: `id__ID,ts2__Timestamp_s_2006-01-02T15:04:05Z07:00_9999-12-31T23:59:59Z_h
0,2433
`, fail: true, intOutOfRange: true, timestampOutOfRange: false, decimalOutOfRange: true},
		{name: "decimalOutOfRange not allowed", csv: `id__ID,price__Decimal_2
0,994492233720368547758.0892233720368547758
`, fail: true, intOutOfRange: true, timestampOutOfRange: true, decimalOutOfRange: false},
		{name: "intOverflow", csv: `id__ID,pospos__Int
0,89273948723984729387492387492987
		`, fail: true, intOutOfRange: false, timestampOutOfRange: false, decimalOutOfRange: false},

		{name: "int string overflow", csv: `id__ID,pospos__Int
0,"89273948723984729387492387492987"
`, fail: true, intOutOfRange: false, timestampOutOfRange: false, decimalOutOfRange: false}}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			m := newMainOORFactory(t, test.csv, test.intOutOfRange, test.decimalOutOfRange, test.timestampOutOfRange)
			m.BatchSize = 1

			defer func() {
				if err := m.PilosaClient().DeleteIndexByName(m.Index); err != nil {
					t.Logf("deleting test index: %v", err)
				}
			}()

			err := m.Run()
			if test.fail {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("running: %v", err)
				}
			}
		})

	}

}

func newMainOORFactory(t *testing.T, file string, allowIntOOR bool, allowDecOOR bool, allowTSOOR bool) *Main {
	name := writeTempFile(t, file)
	m := NewMain()
	m.AutoGenerate = true
	configureTestFlags(m)
	m.Files = []string{name}
	rand.Seed(time.Now().UnixNano())
	m.Index = fmt.Sprintf("oortest%d", rand.Intn(100000))
	m.AllowIntOutOfRange = allowIntOOR
	m.AllowDecimalOutOfRange = allowDecOOR
	m.AllowTimestampOutOfRange = allowTSOOR
	return m
}
