package pilosa_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/ingest"
	"github.com/molecula/featurebase/v2/test"
	"github.com/pkg/errors"
)

// For ingest API testing, we want to do tests which have a known
// schema, no existing data before we start, and perform ingests and
// then do queries.
//
// A good starting point for this would be a fairly simple file
// divided into sections which are just the JSON text of the data
// we want to be working with, or the PQL queries we want to run,
// or their expected results.
//
// So, roughly like this:
//
//     schema:
//     {
//         "index-name": "example",
//         "primary-key-type": "string",
//         "fields": [
//             {
//                 "field-name": "set",
//                 "field-type": "id",
//                 "field-options": { "cache-type": "none" }
//             }
//         ]
//     }
//     ingest:
//     [
//       {
//         "action": "set",
//         "records": {
//           "1": {
//             "set": [ 2 ],
//           }
//         }
//       }
//     ]
//     queries:
//     Row(set=2):
//     [1]
//
// Additionally, the names "schema-error" and "ingest-error" are taken to
// represent a schema, or data set, which is expected to produce an error.
// For instance:
//
//     ingest-error:
//     [ { "action": puppy }
//
// In this case, it would be considered a test failure if an ingest request
// did NOT fail.
// Lines starting with #

// ingestSchemaPartial represents the only part of a schema we need to
// know about in order to undo its creation of a schema for use in a
// test case.
type ingestSchemaPartial struct {
	IndexName string `json:"index-name"`
}

type ingestActionKind int

const (
	ingestActionNone = ingestActionKind(iota)
	ingestActionSchema
	ingestActionIngest
	ingestActionSchemaError
	ingestActionIngestError
	ingestActionQueries
)

var ingestActionKinds = map[string]ingestActionKind{
	"schema":       ingestActionSchema,
	"ingest":       ingestActionIngest,
	"schema-error": ingestActionSchemaError,
	"ingest-error": ingestActionIngestError,
	"queries":      ingestActionQueries,
}
var ingestActionKindNames = map[ingestActionKind]string{}

type testCaseAction struct {
	kind      ingestActionKind
	comment   []byte
	lineStart int
	lineEnd   int
	data      []byte
}

type liner struct {
	data      []byte
	at        int
	start     int
	remaining []byte
	line      []byte
}

func newLiner(data []byte) *liner {
	return &liner{data: data, at: 0, remaining: data}
}

func (l *liner) next() bool {
	if len(l.remaining) == 0 {
		return false
	}
	foundNL := true
	nextNL := bytes.IndexByte(l.remaining, '\n')
	if nextNL == -1 {
		foundNL = false
		nextNL = len(l.remaining)
	}
	l.line = l.remaining[:nextNL]
	// move past the newline we found, if we found one
	if foundNL {
		nextNL++
	}
	l.start, l.at = l.at, l.at+nextNL
	l.remaining = l.data[l.at:]
	return true
}

func (l *liner) text() (line []byte, start int, end int) {
	return l.line, l.start, l.at
}

// parseExpectedResults handles something that looks like
// [1, 2, 3] or ["a", "b", "c"]. It does not handle things like
// quotes within strings, etcetera.
func parseExpectedResults(data []byte) (ints []uint64, keys []string, err error) {
	if len(data) < 2 || data[0] != '[' || data[len(data)-1] != ']' {
		return nil, nil, errors.New("expecting [] results")
	}
	words := bytes.Split(data[1:len(data)-1], []byte{','})
	for _, word := range words {
		word = bytes.TrimSpace(word)
		if len(word) == 0 {
			return nil, nil, errors.New("found empty word expecting result")
		}
		if word[0] == '"' {
			keys = append(keys, string(word[1:len(word)-1]))
			continue
		}
		v, err := strconv.ParseInt(string(word), 10, 64)
		if err != nil {
			return nil, nil, err
		}
		ints = append(ints, uint64(v))
	}
	if len(ints) > 0 && len(keys) > 0 {
		return nil, nil, errors.New("mixed integers and strings are invalid")
	}
	return ints, keys, err
}

func testQueries(t *testing.T, ctx context.Context, cmd *test.Command, index string, action testCaseAction) {
	qcx := cmd.API.Txf().NewQcx()
	defer func() {
		if err := qcx.Finish(); err != nil {
			t.Fatalf("finishing qcx: %v", err)
		}
	}()
	l := newLiner(action.data)
	for l.next() {
		query, _, _ := l.text()
		if !l.next() {
			t.Fatalf("processing query list: no expected after %q", query)
		}
		expected, _, _ := l.text()
		ints, keys, err := parseExpectedResults(expected)
		if err != nil {
			t.Fatalf("processing query list: invalid expected results %q", expected)
		}
		t.Logf("expecting %q -> %s", query, expected)
		res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: index, Query: string(query)})
		if err != nil {
			t.Errorf("query: %v", err)
		}
		if len(res.Results) != 1 {
			t.Fatalf("expected one result per query, got %d results", len(res.Results))
		}
		var row *pilosa.Row
		var ok bool
		if row, ok = res.Results[0].(*pilosa.Row); !ok {
			t.Fatalf("expected results to be a row")
		}
		if ints != nil {
			cols := row.Columns()
			if len(cols) != len(ints) {
				t.Fatalf("wrong number of values, expected %d, got %d", len(ints), len(cols))
			}
			for i := range cols {
				if ints[i] != cols[i] {
					t.Fatalf("result %d: expected %d, got %d", i, ints[i], cols[i])
				}
			}
		}
		// key return value is unpredictable, so...
		if keys != nil {
			seen := make(map[string]struct{})
			for _, k := range keys {
				seen[k] = struct{}{}
			}
			for _, k := range row.Keys {
				if _, ok := seen[k]; !ok {
					t.Fatalf("unexpected result key %q", k)
				}
				delete(seen, k)
			}
			for k := range seen {
				t.Fatalf("expected result to contain %q, but did not get it", k)
			}
		}
	}
}

// testOneIngestTestcase runs a set of actions, then cleans up after itself
func testOneIngestTestcase(t *testing.T, ctx context.Context, cmd *test.Command, tcpath string) {
	data, err := ioutil.ReadFile(tcpath)
	if err != nil {
		t.Fatalf("reading %q: %v", tcpath, err)
	}
	var actions []testCaseAction
	var action testCaseAction
	l := newLiner(data)
	var line []byte
	var start, lineStart, lineEnd int
	lineCount := 0
	for l.next() {
		line, lineStart, lineEnd = l.text()
		lineCount++
		if colon := bytes.IndexByte(line, ':'); colon != -1 {
			if kind, ok := ingestActionKinds[string(line[:colon])]; ok {
				if action.kind != ingestActionNone {
					action.data = data[start:lineStart]
					actions = append(actions, action)
					action.lineEnd = lineCount - 1
				} else {
					if lineStart != 0 {
						t.Logf("warning: %d bytes with no action type before first action", lineStart)
					}
				}
				start = lineEnd
				action.data = nil
				action.kind = kind
				if line[len(line)-1] == ':' {
					action.comment = line[:len(line)-1]
				} else {
					action.comment = line
				}
				action.lineStart = lineCount
			}
		}
	}
	if action.kind != ingestActionNone {
		action.lineEnd = lineCount - 1
		action.data = data[start:]
		actions = append(actions, action)
	}
	var mostRecentIndex string
	seenIndexes := map[string]struct{}{}

	cli := cmd.Client()
	created := map[string][]string{}
	defer func() {
		t.Logf("deleting created indexes/fields:")
		for k, v := range created {
			if len(v) == 0 {
				t.Logf("  index: %q", k)
				if err := cmd.API.DeleteIndex(ctx, k); err != nil {
					t.Errorf("deleting index %q: %v", k, err)
				}
			} else {
				t.Logf("  fields in %q: %q", k, v)
				for _, field := range v {
					if err := cmd.API.DeleteField(ctx, k, field); err != nil {
						t.Errorf("deleting field %q from %q: %v", field, k, err)
					}
				}
			}
		}
	}()

	noticeCreation := func(newlyCreated map[string][]string) {
		for k, v := range newlyCreated {
			if len(v) == 0 {
				if existing, ok := created[k]; ok {
					if len(existing) > 0 {
						t.Fatalf("creation reports index %q newly created, but we created fields %q in it previously",
							k, existing)
					}
				}
				// create an empty list, indicating that the whole index is
				// believed nil
				created[k] = nil
				continue
			}
			if existing, ok := created[k]; ok {
				if len(existing) == 0 {
					// we'll delete this index anyway, don't need to delete fields in it
					continue
				}
				created[k] = append(existing, v...)
				continue
			}
			created[k] = v
		}
	}

	for _, action := range actions {
		t.Logf("%s, lines %d-%d", action.comment, action.lineStart, action.lineEnd)
		switch action.kind {
		case ingestActionSchema:
			var scratch ingestSchemaPartial
			err = json.Unmarshal(action.data, &scratch)
			if err != nil {
				t.Fatalf("couldn't parse schema data: %v", err)
			}
			if scratch.IndexName == "" {
				t.Fatalf("test case must provide an index name")
			}
			// stash the string from the schema, because we
			// might need it later
			mostRecentIndex = scratch.IndexName
			seenIndexes[mostRecentIndex] = struct{}{}
			var newlyCreated map[string][]string
			newlyCreated, err = cli.IngestSchema(ctx, nil, action.data)
			if err != nil {
				t.Fatalf("executing schema: %v", err)
			}
			noticeCreation(newlyCreated)
		case ingestActionSchemaError:
			var scratch ingestSchemaPartial
			err = json.Unmarshal(action.data, &scratch)
			if err != nil {
				t.Logf("got expected error from schema: %v", err)
				break
			}
			var newlyCreated map[string][]string
			newlyCreated, err = cli.IngestSchema(ctx, nil, action.data)
			if err != nil {
				t.Logf("got expected error from schema: %v", err)
				break
			}
			noticeCreation(newlyCreated)
			t.Fatalf("expected error from schema, didn't get it")
		case ingestActionIngest:
			func() {
				qcx := cmd.API.Txf().NewQcx()
				var err error
				defer func() {
					if err == nil {
						qcx.Abort()
						return
					}
					if err := qcx.Finish(); err != nil {
						t.Fatalf("finishing qcx: %v", err)
					}
				}()
				err = cmd.API.IngestOperations(ctx, qcx, mostRecentIndex, bytes.NewBuffer(action.data))
				if err != nil {
					t.Fatalf("importing data: %v", err)
				}
			}()
		case ingestActionIngestError:
			func() {
				qcx := cmd.API.Txf().NewQcx()
				var err error
				defer func() {
					if err == nil {
						qcx.Abort()
						return
					}
					if err := qcx.Finish(); err != nil {
						t.Fatalf("finishing qcx: %v", err)
					}
				}()
				err = cmd.API.IngestOperations(ctx, qcx, mostRecentIndex, bytes.NewBuffer(action.data))
				if err != nil {
					t.Logf("got expected error from ingest: %v", err)
					return
				}
				t.Fatalf("expected error from ingest, didn't get it")
			}()
		case ingestActionQueries:
			testQueries(t, ctx, cmd, mostRecentIndex, action)
		}
	}

}

// TestIngestTestcases reads sample test cases from a test data directory
// and evaluates them.
func TestIngestTestcases(t *testing.T) {
	_ = &ingest.Operation{}
	var testcases []string
	if len(ingestActionKindNames) == 0 {
		for k, v := range ingestActionKinds {
			ingestActionKindNames[v] = k
		}
	}
	err := filepath.Walk("ingest_testdata", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".tc") {
			testcases = append(testcases, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("looking for test cases: %v", err)
	}
	if len(testcases) == 0 {
		t.Fatalf("no ingest test cases found")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	coord := c.GetPrimary()

	for _, tc := range testcases {
		t.Run(strings.TrimSuffix(tc, ".tc"), func(t *testing.T) {
			testOneIngestTestcase(t, ctx, coord, tc)
		})
	}
}
