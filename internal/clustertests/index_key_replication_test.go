// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clustertest

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/disco"
	"github.com/molecula/featurebase/v2/http"
	picli "github.com/molecula/featurebase/v2/http"
	"github.com/molecula/featurebase/v2/net"
	"github.com/molecula/featurebase/v2/topology"
)

// index -> key -> ids from all replicas
type translationRes map[string]map[string][]uint64

func defaultTranslationResults(indexes []string) translationRes {
	res := make(translationRes)
	for _, index := range indexes {
		res[index] = make(map[string][]uint64)
	}
	return res
}

func verify(allRes translationRes, indexes []string, replicasN, count int) error {
	received := 0
	allIDsSame := func(ids []uint64) bool {
		if len(ids) <= 1 {
			// trivially true
			return true
		}
		id := ids[0]
		for _, other := range ids {
			if id != other {
				return false
			}
		}
		return true
	}
	for _, index := range indexes {
		res, ok := allRes[index]
		if !ok {
			return fmt.Errorf("Expected index '%s' but not present", index)
		}
		for key, ids := range res {
			// first id is after successful translation, rest are from
			// translate readers
			replicationCount := len(ids) - 1
			received += replicationCount
			if replicationCount != replicasN {
				return fmt.Errorf("Count for ids for key '%s'(%d), index '%s' not equal than replicasN(%d)", key, replicationCount, index, replicasN)
			}
			if !allIDsSame(ids) {
				return fmt.Errorf("Expected all ids for key '%s', index '%s' to be the same across the cluster %+v", key, index, ids)
			}
		}
	}

	if received != count {
		return fmt.Errorf("Expected %d count of keys, received %d", count, received)
	}

	return nil
}
func getURIsFromAddresses(addrs []string) ([]*net.URI, error) {
	uris := make([]*net.URI, 0, len(addrs))
	for _, addr := range addrs {
		uri, err := net.NewURIFromAddress(addr)
		if err != nil {
			return nil, err
		}
		uris = append(uris, uri)
	}
	return uris, nil
}

func getClients(addrs []string) ([]*http.InternalClient, error) {
	clients := make([]*http.InternalClient, 0, len(addrs))
	for _, addr := range addrs {
		c, err := picli.NewInternalClient(addr, picli.GetHTTPClient(nil))
		if err != nil {
			return nil, err
		}
		clients = append(clients, c)
	}
	return clients, nil
}

func genIndexNames(indexCount int) []string {
	indexNames := make([]string, 0, indexCount)
	for i := 1; i <= indexCount; i++ {
		indexNames = append(indexNames, fmt.Sprintf("idx-%d", i))
	}
	return indexNames
}

func parseDuration(t *testing.T, s string) time.Duration {
	parsed, err := time.ParseDuration(s)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func durationToSeconds(d time.Duration) string {
	s := int(d.Seconds())
	return fmt.Sprintf("%ds", s)
}

func TestIndexKeyReplication(t *testing.T) {
	if os.Getenv("ENABLE_PILOSA_CLUSTER_TESTS_FOR_INDEX_KEY_REPLICATION") != "1" {
		t.Skip("pilosa cluster tests for index key replication are not enabled")
	}
	// configurations for test
	replicasN := 3
	indexCount := 4
	intervalDurationArg := "100ms"
	totalInsertionDurationArg := "10s"
	coolOffDurationArg := "5s"
	numKeysToInsertPerDuration := 100
	addresses := []string{"pilosa1:10101", "pilosa2:10101", "pilosa3:10101"}

	intervalDuration := parseDuration(t, intervalDurationArg)
	totalInsertionDuration := parseDuration(t, totalInsertionDurationArg)
	coolOffDuration := parseDuration(t, coolOffDurationArg)

	indexes := genIndexNames(indexCount)
	clients, err := getClients(addresses)
	cli := clients[0]
	if err != nil {
		t.Fatalf("on init clients from addresses: %v, %v", addresses, err)
	}
	uris, err := getURIsFromAddresses(addresses)
	if err != nil {
		t.Fatalf("on init clients from addresses: %v, %v", addresses, err)
	}
	ctx := context.Background()

	// create index keyed
	for _, index := range indexes {
		err = cli.EnsureIndex(ctx, index, pilosa.IndexOptions{
			Keys: true,
		})
		if err != nil {
			t.Fatalf("creating/asserting index: %v", err)
		}
	}

	// set up index keys to insert
	keysInserted := 0
	allRes := defaultTranslationResults(indexes)
	{
		ctxForIndexKeyCreation, cancelFurtherInsertions := context.WithCancel(ctx)
		var wg sync.WaitGroup
		var mu = &sync.Mutex{}
		wg.Add(len(indexes))
		seed := time.Now().UnixNano()
		t.Logf("start inserting index keys for %v, seed(%v)", totalInsertionDuration, seed)
		rng := rand.New(rand.NewSource(seed))
		for _, index := range indexes {
			translations := allRes[index]
			go func(index string, translations map[string][]uint64) {
				keys := make([]string, numKeysToInsertPerDuration)
				offset := 1
				ticker := time.NewTicker(intervalDuration)
				defer func() {
					ticker.Stop()
					wg.Done()
				}()
				for {
					select {
					case <-ticker.C:
						for i := 0; i < numKeysToInsertPerDuration; i++ {
							keys[i] = fmt.Sprintf("key-%d", i+offset)
						}
						// pick random node to send key creation to
						r := rng.Intn(len(addresses))
						cli, uri := clients[r], uris[r]

						// insert index keys
						transmap, err := cli.CreateIndexKeysNode(ctxForIndexKeyCreation, uri, index, keys...)
						if err != nil {
							if err == context.Canceled {
								return
							} else {
								t.Logf("creating index keys for index(%s) send to node(%s): %v", index, uri.String(), err)
								continue
							}
						}
						for key, id := range transmap {
							translations[key] = append(translations[key], id)
						}
						mu.Lock()
						keysInserted += len(transmap)
						mu.Unlock()
						offset += numKeysToInsertPerDuration
					case <-ctxForIndexKeyCreation.Done():
						return
					}
				}
			}(index, translations)
		}
		// inject fault
		pcmd := exec.Command("/pumba", "netem", "--duration",
			durationToSeconds(totalInsertionDuration),
			"loss", "--percent", "50", "--correlation", "60",
			"clustertests_pilosa3_1")
		pcmd.Stdout = os.Stdout
		pcmd.Stderr = os.Stderr
		t.Logf("sending pumba fault injection cmd: %v", pcmd.String())
		err = pcmd.Start()
		if err != nil {
			t.Fatalf("starting pumba command: %v", err)
		}
		err = pcmd.Wait()
		if err != nil {
			t.Fatalf("waiting on pumba pause cmd: %v", err)
		}

		// wait for index keys to be created
		t.Logf("start wait to complete index key creation")
		time.Sleep(totalInsertionDuration)
		cancelFurtherInsertions()
		wg.Wait()
		t.Logf("done with inserting index keys. Total keys inserted: %d", keysInserted)
	}

	t.Logf("start cool off period: %v\n", coolOffDuration)
	time.Sleep(coolOffDuration)
	t.Log("done with cool off period, waiting for stability")
	waitForStatus(t, clients[0].Status, string(disco.ClusterStateNormal), 30, 1*time.Second)
	t.Log("done with waiting for stability, starting verifying persistence of index keys")

	// get all nodes
	nodes, err := cli.Nodes(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// prepare translate offset maps
	nodeMaps := make(map[string]pilosa.TranslateOffsetMap)
	for _, n := range nodes {
		nodeMaps[n.ID] = make(pilosa.TranslateOffsetMap)
	}
	schema, err := cli.Schema(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, indexInfo := range schema {
		index := indexInfo.Name
		isKeyed := indexInfo.Options.Keys
		if !isKeyed {
			continue
		}

		partitionN := topology.DefaultPartitionN
		for partition := 0; partition < partitionN; partition++ {
			nodes, err := cli.PartitionNodes(ctx, partition)
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range nodes {
				m := nodeMaps[n.ID]
				m.SetIndexPartitionOffset(index, partition, 0)
			}
		}
	}

	// open translate reader
	readers := make([]pilosa.TranslateEntryReader, 0, len(nodes))
	closeAllTranslateReaders := func() []error {
		var errs []error
		for _, tr := range readers {
			err := tr.Close()
			if err != nil {
				errs = append(errs, err)
			}
		}
		return errs
	}
	var tlsConfig *tls.Config = nil
	var wg sync.WaitGroup
	entries := make(chan *pilosa.TranslateEntry)
	for _, n := range nodes {
		client := http.GetHTTPClient(tlsConfig)
		nodeURL := n.URI.String()
		offsets := nodeMaps[n.ID]
		openTranslateReader := http.GetOpenTranslateReaderFunc(client)
		tr, err := openTranslateReader(ctx, nodeURL, offsets)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		readers = append(readers, tr)
		go func(node *topology.Node, tr pilosa.TranslateEntryReader) {
			defer func() {
				wg.Done()
			}()
			for {
				var entry pilosa.TranslateEntry
				err := tr.ReadEntry(&entry)
				if err != nil {
					// TODO also ignore transient http: read on clsoed response body errors
					// for now just print error
					if err != context.Canceled {
						fmt.Printf("node(%s) On read from translate entry reader: %v", node.URI.String(), err)
					}
					return
				}
				// ignore field keys
				if entry.Field != "" {
					continue
				}
				entries <- &entry
			}
		}(n, tr)
	}

	// receive all translation entries made
	count := replicasN * keysInserted
	i := 0
	for {
		entry := <-entries
		res, ok := allRes[entry.Index]
		if !ok {
			// ignore indexes we did not create for this test
			continue
		}
		ids, ok := res[entry.Key]
		if !ok {
			// ignore keys we did not insert for the test
			continue
		}
		res[entry.Key] = append(ids, entry.ID)
		i++
		if i == count {
			break
		}
	}
	errs := closeAllTranslateReaders()
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Errorf("on close translate readers: %v", err)
		}
	}
	close(entries)

	err = verify(allRes, indexes, replicasN, count)
	if err != nil {
		t.Fatal(err)
	}
}
