package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/molecula/featurebase/v2"
	phttp "github.com/molecula/featurebase/v2/http"
	"golang.org/x/sync/errgroup"
)

var (
	requestCountVar          = expvar.NewInt("request_count")
	requestCurrentLatencyVar = expvar.NewFloat("request_current_latency") // seconds
	requestAvgLatencyVar     = expvar.NewFloat("request_avg_latency")     // seconds
	requestTotalLatencyVar   = expvar.NewFloat("request_total_latency")   // seconds
	requestPerSecVar         = expvar.NewFloat("request_per_sec")
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err == flag.ErrHelp {
		os.Exit(1)
	} else if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("pilosa-bench", flag.ContinueOnError)
	hostport := fs.String("hostport", "localhost:10101", "")
	typ := fs.String("type", "row", "query type (row)")
	n := fs.Int("n", 1000, "number of queries")
	rate := fs.Int("rate", 1, "number of queries per second")
	verbose := fs.Bool("v", false, "verbose logging")
	from := fs.String("from", "", "from time for row-range queries (ISO 8601)")
	to := fs.String("to", "", "to time for row-range queries (ISO 8601)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Parse from/to time.
	var opt queryOptions
	if *from != "" {
		if opt.from, err = time.Parse(time.RFC3339, *from); err != nil {
			return fmt.Errorf("cannot parse -from time")
		}
	}
	if *to != "" {
		if opt.to, err = time.Parse(time.RFC3339, *to); err != nil {
			return fmt.Errorf("cannot parse -to time")
		}
	}

	if (*typ == "row-range" || *typ == "topk") && (opt.from.IsZero() || opt.to.IsZero()) {
		return fmt.Errorf("-from and -to flags must be specified for topk & row-range queries")
	}

	// Clear time prefix on log.
	log.SetFlags(0)
	if !*verbose {
		log.SetOutput(ioutil.Discard)
	}

	// Setup PRNG to have consistent values for the same set of data.
	rand.Seed(0)

	// Setup connection to pilosa.
	client, err := phttp.NewInternalClient(*hostport, http.DefaultClient)
	if err != nil {
		return err
	}

	// Set up HTTP endpoint to provide /debug endpoints.
	fmt.Println("Serving debug endpoint at http://localhost:7070/debug")
	go func() { _ = http.ListenAndServe(":7070", nil) }()

	// Run separate goroutine to calculate the current req/sec & latency.
	go monitor()

	// Load all id/keys for each field.
	log.Printf("loading field identifiers")
	fieldIDMap, err := loadFields(ctx, client)
	if err != nil {
		return fmt.Errorf("cannot load field identifiers: %w", err)
	} else if len(fieldIDMap) == 0 {
		return fmt.Errorf("no field identifiers available, please verify data exists")
	}

	// Generate list of sorted keys.
	fieldKeys := make([]fieldKey, 0, len(fieldIDMap))
	for k, f := range fieldIDMap {
		switch *typ {
		case "row-bsi":
			if f.info.Options.Type != "int" {
				continue
			}
		case "row-range", "topk":
			if f.info.Options.Type != "time" {
				continue
			}
		default:
			if f.info.Options.Type == "int" || f.info.Options.Type == "time" {
				continue
			}
		}

		fieldKeys = append(fieldKeys, k)
	}
	sort.Slice(fieldKeys, func(i, j int) bool {
		return compareFieldKeys(fieldKeys[i], fieldKeys[j]) == -1
	})

	// Ensure we have appropriate fields for our query type.
	if len(fieldKeys) == 0 {
		return fmt.Errorf("no available fields are appropriate for %q queries", *typ)
	}

	log.Printf("issuing %d queries at %d query/sec", *n, *rate)

	// Repeatedly issue queries based on available row data.
	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	var g errgroup.Group
	for i := 0; i < *n; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		key := fieldKeys[rand.Intn(len(fieldKeys))]
		q, err := generateQuery(*typ, key.index, key.field, fieldIDMap[key].info, fieldIDMap[key].identifiers, opt)
		if err != nil {
			return fmt.Errorf("cannot generate query: %w", err)
		}

		log.Printf("[query] %s", q)

		g.Go(func() error {
			t := time.Now()
			_, err = client.Query(ctx, key.index, &pilosa.QueryRequest{Index: key.index, Query: q})
			if err != nil {
				return err
			}
			elapsed := time.Since(t).Seconds()
			requestCountVar.Add(1)
			requestTotalLatencyVar.Add(elapsed)
			requestAvgLatencyVar.Set(requestTotalLatencyVar.Value() / float64(requestCountVar.Value()))
			return nil
		})
	}

	return g.Wait()
}

// monitor runs in a separate goroutine and updates metrics.
func monitor() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastTime time.Time
	var lastN int64
	var lastLatency float64
	for range ticker.C {
		now, n := time.Now(), requestCountVar.Value()
		latency := requestTotalLatencyVar.Value()

		if !lastTime.IsZero() {
			elapsed := lastTime.Sub(now).Seconds()
			if n > 0 {
				requestCurrentLatencyVar.Set((lastLatency - latency) / float64(n))
			}
			requestPerSecVar.Set(float64(lastN-n) / elapsed)
		}
		lastTime, lastN, lastLatency = now, n, latency
	}
}

func generateQuery(typ, index, field string, info *pilosa.FieldInfo, identifiers *pilosa.RowIdentifiers, opt queryOptions) (string, error) {
	switch typ {
	case "row":
		return generateRowQuery(index, field, identifiers), nil
	case "row-bsi":
		return generateRowBSIQuery(index, field), nil
	case "row-range":
		return generateRowRangeQuery(index, field, identifiers, opt.from, opt.to), nil
	case "count":
		return generateCountQuery(index, field, identifiers), nil
	case "intersect":
		return generateIntersectQuery(index, field, identifiers), nil
	case "union":
		return generateUnionQuery(index, field, identifiers), nil
	case "difference":
		return generateDifferenceQuery(index, field, identifiers), nil
	case "xor":
		return generateXorQuery(index, field, identifiers), nil
	case "groupby":
		return generateGroupByQuery(index, field), nil
	case "topk":
		return generateTopKQuery(index, field, opt.from, opt.to), nil
	default:
		return "", fmt.Errorf("invalid query type: %q", typ)
	}
}

func generateRowQuery(index, field string, identifiers *pilosa.RowIdentifiers) string {
	if len(identifiers.Rows) > 0 {
		return fmt.Sprintf("Row(%s=%d)", field, chooseRowID(identifiers))
	}
	return fmt.Sprintf("Row(%s=%q)", field, chooseRowKey(identifiers))
}

func generateRowBSIQuery(index, field string) string {
	return fmt.Sprintf("Row(%s > 0)", field)
}

func generateRowRangeQuery(index, field string, identifiers *pilosa.RowIdentifiers, from, to time.Time) string {
	if len(identifiers.Rows) > 0 {
		return fmt.Sprintf("Row(%s=%d, from='%s', to='%s')", field, chooseRowID(identifiers), from.Format("2006-01-02T15:04"), to.Format("2006-01-02T15:04"))
	}
	return fmt.Sprintf("Row(%s=%q, from='%s', to='%s')", field, chooseRowKey(identifiers), from.Format("2006-01-02T15:04"), to.Format("2006-01-02T15:04"))
}

func generateRowQueries(index, field string, identifiers *pilosa.RowIdentifiers) string {
	a := make([]string, rand.Intn(9)+1)
	for i := range a {
		a[i] = generateRowQuery(index, field, identifiers)
	}
	return strings.Join(a, ", ")
}

func generateCountQuery(index, field string, identifiers *pilosa.RowIdentifiers) string {
	return fmt.Sprintf("Count(%s)", generateRowQuery(index, field, identifiers))
}

func generateIntersectQuery(index, field string, identifiers *pilosa.RowIdentifiers) string {
	return fmt.Sprintf("Intersect(%s)", generateRowQueries(index, field, identifiers))
}

func generateUnionQuery(index, field string, identifiers *pilosa.RowIdentifiers) string {
	return fmt.Sprintf("Union(%s)", generateRowQueries(index, field, identifiers))
}

func generateDifferenceQuery(index, field string, identifiers *pilosa.RowIdentifiers) string {
	return fmt.Sprintf("Difference(%s)", generateRowQueries(index, field, identifiers))
}

func generateXorQuery(index, field string, identifiers *pilosa.RowIdentifiers) string {
	return fmt.Sprintf("Xor(%s)", generateRowQueries(index, field, identifiers))
}

func generateGroupByQuery(index, field string) string {
	return fmt.Sprintf("GroupBy(Rows(%s))", field)
}

func generateTopKQuery(index, field string, from, to time.Time) string {
	return fmt.Sprintf("TopK(%s, from='%s', to='%s')", field, from.Format("2006-01-02T15:04"), to.Format("2006-01-02T15:04"))
}

// loadFields returns a mapping of index/field names to field info & identifiers.
func loadFields(ctx context.Context, client *phttp.InternalClient) (map[fieldKey]*fieldInfo, error) {
	indexes, err := client.Schema(ctx)
	if err != nil {
		return nil, err
	}

	m := make(map[fieldKey]*fieldInfo)
	for _, ii := range indexes {
		for _, f := range ii.Fields {
			log.Printf("field: index=%s name=%s type=%s", ii.Name, f.Name, f.Options.Type)

			switch f.Options.Type {
			case "set", "mutex", "time":
				identifiers, err := fetchFieldIDs(ctx, client, ii.Name, f.Name)
				if err != nil {
					return nil, fmt.Errorf("fetch fields: %w", err)
				} else if len(identifiers.Rows) > 0 || len(identifiers.Keys) > 0 {
					m[fieldKey{ii.Name, f.Name}] = &fieldInfo{f, identifiers}
				}

			case "int":
				m[fieldKey{ii.Name, f.Name}] = &fieldInfo{info: f}
			}
		}
	}
	return m, nil
}

// fetchFieldIDs returns a list of field IDs or keys.
func fetchFieldIDs(ctx context.Context, client *phttp.InternalClient, indexName, fieldName string) (*pilosa.RowIdentifiers, error) {
	resp, err := client.Query(ctx, indexName, &pilosa.QueryRequest{Index: indexName, Query: `Rows(` + fieldName + `)`})
	if err != nil {
		return nil, err
	}

	switch result := resp.Results[0].(type) {
	case *pilosa.RowIdentifiers:
		return result, nil
	case pilosa.RowIdentifiers:
		return &result, nil
	default:
		return nil, fmt.Errorf("unexpected result type: %T", result)
	}
}

func chooseRowID(identifiers *pilosa.RowIdentifiers) uint64 {
	return identifiers.Rows[rand.Intn(len(identifiers.Rows))]
}

func chooseRowKey(identifiers *pilosa.RowIdentifiers) string {
	return identifiers.Keys[rand.Intn(len(identifiers.Keys))]
}

type fieldKey struct {
	index string
	field string
}

type fieldInfo struct {
	info        *pilosa.FieldInfo
	identifiers *pilosa.RowIdentifiers
}

func compareFieldKeys(x, y fieldKey) int {
	if cmp := strings.Compare(x.index, y.index); cmp != 0 {
		return cmp
	}
	return strings.Compare(x.field, y.field)
}

type queryOptions struct {
	from, to time.Time
}
