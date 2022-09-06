package idk

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"sync"
	"testing"
	"time"

	pilosacore "github.com/featurebasedb/featurebase/v3"
	"github.com/pkg/errors"
)

type spyNexter struct {
	nexter IDAllocator
}

func (sn *spyNexter) Next(ctx context.Context, rec Record) (uint64, error) {
	id, err := sn.nexter.Next(ctx, rec)
	if err != nil {
		return id, err
	}
	oRec, ok := rec.(*offsetRecord)
	if !ok {
		return 0, fmt.Errorf("Invalid usage of spyNexter with wrong record type")
	}
	// some other ingester might be nexting the same record concurrently
	// so we need to serialize modifications
	oRec.mu.Lock()
	defer oRec.mu.Unlock()
	oRec.idsAllocated = append(oRec.idsAllocated, id)
	return id, err
}

func (sn *spyNexter) Reserve(ctx context.Context, count uint64) error {
	return sn.nexter.Reserve(ctx, count)
}

func (sn *spyNexter) Commit(ctx context.Context) error {
	return sn.nexter.Commit(ctx)
}

func TestOffsetBasedIDAllocation(t *testing.T) {
	seed := time.Now().UTC().UnixNano()
	rand.Seed(seed)

	schema := []Field{IDField{NameVal: "aval"}}

	numIngesters := 3
	numPartitions := 10
	recordsPerPartition := 10
	batchSize := 4
	index := fmt.Sprintf("idallocation%d", rand.Intn(10000000))
	rng := rand.New(rand.NewSource(seed))

	sources := genSources(rng, schema, numIngesters, numPartitions, recordsPerPartition)

	// create ingesters
	type ingester struct {
		m        *Main
		c        int
		onFinish func()
		err      error
	}

	var ingesters []*ingester
	for i := 0; i < numIngesters; i++ {
		m, onFinish, err := spawnIngester(i, index, sources[i], batchSize)
		if err != nil {
			t.Fatal(err)
		}

		ingesters = append(ingesters, &ingester{
			m:        m,
			onFinish: onFinish,
			c:        i,
		})
	}

	defer func() {
		if err := ingesters[0].m.PilosaClient().DeleteIndexByName(ingesters[0].m.Index); err != nil {
			t.Logf("deleting test index: %v", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(numIngesters)
	for _, ingester := range ingesters {
		ingester := ingester
		go func() {
			defer func() {
				ingester.onFinish()
				wg.Done()
			}()

			c := ingester.c
			l := &msgCounter{MaxMsgs: 100}
			err := ingester.m.runIngester(c, l)
			ingester.err = err
		}()
	}

	wg.Wait()

	// check if any ingester exited with an error
	for _, i := range ingesters {
		if i.err != nil {
			t.Fatal(i.err)
		}
	}

	// throw all records to set to avoid reprocessing them multiple
	// times
	records := make(map[*offsetRecord]struct{})
	for _, s := range sources {
		for _, r := range s.records {
			records[r] = struct{}{}
		}
	}

	// for each record make sure all ids allocated are the same
	for r := range records {
		if !allValsSame(r.idsAllocated) {
			rid := fmt.Sprintf("partition: %s, offset %d", r.groupKey, r.offset)
			errMsg := fmt.Sprintf("For record(%s) ids allocated by nexter not all same: %v", rid, r.idsAllocated)
			t.Fatal(errMsg)
		}
	}

}

func allValsSame(nums []uint64) bool {
	if len(nums) <= 1 {
		return true // trivially true
	}
	num := nums[0]
	for _, n := range nums[1:] {
		if n != num {
			return false
		}
	}
	return true
}

func spawnIngester(c int, index string, ts Source, batchSize int) (ingester *Main, onFinish func(), err error) {
	ingester = NewMain()

	// use external, offset mode id allocation
	ingester.AutoGenerate = true
	ingester.ExternalGenerate = true
	ingester.OffsetMode = true

	configureTestFlags(ingester)
	ingester.NewSource = func() (Source, error) { return ts, nil }
	ingester.Index = index
	ingester.BatchSize = batchSize
	// ingester.LogPath = "/dev/null"

	onFinishRun, err := ingester.Setup()
	if err != nil {
		return nil, nil, errors.Wrap(err, "setting up")
	}

	// hijack nexter with our very own spyNexter
	currNewNexter := ingester.newNexter
	ingester.newNexter = func(c int) (IDAllocator, error) {
		nexter, err := currNewNexter(c)
		if _, ok := nexter.(*offsetModeNexter); !ok {
			return nil, fmt.Errorf("Invalid nexter type, should be offsetBasedNexter")
		}
		sNexter := &spyNexter{
			nexter: nexter,
		}
		return sNexter, err
	}

	return ingester, onFinishRun, nil
}

// RECORD
type offsetRecord struct {
	groupKey     string
	idsAllocated []uint64
	dup          int
	numTimesRead int
	offset       int
	mu           sync.Mutex
	data         []interface{}
}

func (r *offsetRecord) Commit(ctx context.Context) error {
	return nil
}

func (r *offsetRecord) StreamOffset() (string, uint64) {
	return r.groupKey, uint64(r.offset)
}

var _ OffsetStreamRecord = &offsetRecord{}

func (r *offsetRecord) Data() []interface{} {
	return r.data
}

type offsetTestSource struct {
	schema  []Field
	records []*offsetRecord
	i       int
}

func (s *offsetTestSource) Close() error {
	return nil
}

func (s *offsetTestSource) Schema() []Field {
	return s.schema
}

func (s *offsetTestSource) Record() (Record, error) {
	s.i++
	if s.i <= len(s.records) {
		return s.records[s.i-1], nil
	}
	return nil, io.EOF
}

func genSources(rng *rand.Rand, schema []Field, numSources int, numPartitions int, recordsPerPartition int) []*offsetTestSource {
	// generate records , each records has a dup value
	// which determines how many times it will be read
	var records []*offsetRecord
	for pid := 0; pid < numPartitions; pid++ {
		key := fmt.Sprintf("partition-%d", pid)
		for i := 0; i < recordsPerPartition; i++ {
			dup := rng.Intn(5) + 1
			rec := &offsetRecord{
				offset:       i,
				groupKey:     key,
				data:         []interface{}{i},
				dup:          dup,
				numTimesRead: 0,
			}
			records = append(records, rec)
		}
	}

	// shuffle records then sort based on offset
	// so that different partitions can go to different
	// ingesters
	rng.Shuffle(len(records), func(i, j int) {
		records[i], records[j] = records[j], records[i]
	})
	sort.Slice(records, func(i, j int) bool {
		return records[i].offset < records[j].offset
	})

	// create sources
	var sources []*offsetTestSource
	for i := 0; i < numSources; i++ {
		sources = append(sources, &offsetTestSource{
			schema: schema,
		})
	}

	// distribute records to sources, round-robin-ish
	sourceIndex := 0
	curr := make([]*offsetRecord, len(records))
	copy(curr, records[:])
	for len(curr) > 0 {
		var next []*offsetRecord
		for _, r := range curr {
			source := sources[sourceIndex]
			source.records = append(source.records, r)
			r.numTimesRead++
			if r.numTimesRead == r.dup {
				r.numTimesRead = 0
			} else {
				next = append(next, r)
			}
			sourceIndex = (sourceIndex + 1) % numSources
		}
		curr = next
	}

	return sources
}

func Test_pilosaIDManager_commit(t *testing.T) {
	token := "Bearer " + getAuthToken(t)
	ctx := context.WithValue(context.Background(), contextKeyToken, token)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok, ok := r.Header["Authorization"]

		if !ok || len(tok) == 0 {
			t.Fatal("Authorization Header not found")
		}

		if tok[0] != token {
			t.Fatalf("expected: %s, got: %s", token, tok)
		}
	}))

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal("Could not parse URL")
	}

	ps := pilosaIDManager{
		hcli: srv.Client(),
		url:  *u,
	}

	_ = ps.commit(ctx, pilosacore.IDAllocCommitRequest{})
}

func Test_pilosaIDManager_reserve(t *testing.T) {
	token := "Bearer " + getAuthToken(t)
	ctx := context.WithValue(context.Background(), contextKeyToken, token)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok, ok := r.Header["Authorization"]

		if !ok || len(tok) == 0 {
			t.Fatal("Authorization Header not found")
		}

		if tok[0] != token {
			t.Fatalf("expected: %s, got: %s", token, tok)
		}
	}))

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal("Could not parse URL")
	}

	ps := pilosaIDManager{
		hcli: srv.Client(),
		url:  *u,
	}

	_, _ = ps.reserve(ctx, pilosacore.IDAllocReserveRequest{})
}
