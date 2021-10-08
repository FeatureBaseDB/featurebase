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
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v2"
	boltdb "github.com/molecula/featurebase/v2/boltdb"
	"github.com/molecula/featurebase/v2/disco"
	"github.com/molecula/featurebase/v2/http"
	picli "github.com/molecula/featurebase/v2/http"
	"github.com/molecula/featurebase/v2/net"
	"github.com/molecula/featurebase/v2/topology"
	"github.com/pkg/errors"
)

func sendCmd(cmd string, args ...string) error {
	pcmd := exec.Command(cmd, args...)
	pcmd.Stdout = os.Stdout
	pcmd.Stderr = os.Stderr
	err := pcmd.Start()
	if err != nil {
		return errors.Wrap(err, "starting cmd")
	}
	err = pcmd.Wait()
	if err != nil {
		return errors.Wrap(err, "waiting on cmd")
	}
	return nil
}

func unpauseNode(node string) error {
	unpauseArgs := []string{"container", "unpause", "clustertests_" + node + "_1"}
	return sendCmd("docker", unpauseArgs...)
}

func pauseNode(node string) error {
	pauseArgs := []string{"container", "pause", "clustertests_" + node + "_1"}
	return sendCmd("docker", pauseArgs...)
}

type keyInserter struct {
	client *http.InternalClient
	uri    *net.URI
	index  string
	keys   []string
}

func (ki keyInserter) insertKeys(ctx context.Context) (map[string]uint64, error) {
	ts, err := ki.client.CreateIndexKeysNode(ctx, ki.uri, ki.index, ki.keys...)
	return ts, err
}

func getAddress(node string) string {
	return node + ":10101"
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

func readIndexTranslateData(ctx context.Context, client *picli.InternalClient, dirPath, index string, partition int) error {
	// read translateStore contents from endpoint
	r, err := client.IndexTranslateDataReader(ctx, index, partition)
	if err != nil {
		return err
	}
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	r.Close()

	// create file and write contents to it
	filename := strconv.FormatInt(int64(partition), 10)
	filePath := filepath.Join(dirPath, filename)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	_, err = file.Write(buf)
	if err != nil {
		file.Close()
		return err
	}
	err = file.Sync()
	if err != nil {
		file.Close()
		return err
	}
	file.Close()
	return nil
}

func openTranslateStores(dirPath, index string) (map[int]pilosa.TranslateStore, error) {
	dirEntries, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	// in case of error, close any translateStore that has been opened
	rollback := make([]pilosa.TranslateStore, 0, topology.DefaultPartitionN)
	defer func() {
		for _, ts := range rollback {
			_ = ts.Close()
		}
	}()

	// filter out non-file entries
	filePaths := make([]string, 0, len(dirEntries))
	for _, entry := range dirEntries {
		if entry.Mode().IsDir() {
			continue
		}
		filePath := filepath.Join(dirPath, entry.Name())
		filePaths = append(filePaths, filePath)
	}

	translateStores := make(map[int]pilosa.TranslateStore)
	for _, filePath := range filePaths {
		// extract partition number from filename
		file := filepath.Base(filePath)
		partition, err := strconv.Atoi(file)
		if err != nil {
			return nil, err
		}
		// open bolt db
		ts, err := boltdb.OpenTranslateStore(filePath, index, "", partition, topology.DefaultPartitionN, false)
		ts.SetReadOnly(true)
		if err != nil {
			return nil, err
		}
		translateStores[partition] = ts
		rollback = append(rollback, ts)
	}
	rollback = nil

	return translateStores, nil
}

var errOpRetriable = errors.New("If operation failed on this error, it can be retried")

func verifyNodeHasGivenKeys(ctx context.Context, node, index, dirPath string, keys []string) error {
	// get client that's connected to node
	address := getAddress(node)
	client, err := picli.NewInternalClient(address, picli.GetHTTPClient(nil))
	if err != nil {
		return err
	}

	// create dir to store boltdbs for this node
	nodeDirPath := filepath.Join(dirPath, node)
	err = os.Mkdir(nodeDirPath, 0755)
	if err != nil {
		return err
	}

	// read in all the translate stores for each partition
	for partition := 0; partition < topology.DefaultPartitionN; partition++ {
		err := readIndexTranslateData(ctx, client, nodeDirPath, index, partition)
		if err != nil {
			return err
		}
	}

	// open all the translate stores
	translateStores, err := openTranslateStores(nodeDirPath, index)
	if err != nil {
		return err
	}

	// close all the translate stores on complete
	defer func() {
		for _, ts := range translateStores {
			ts.Close()
		}
	}()

	// merge all translations
	merged := make(map[string]uint64)
	for _, ts := range translateStores {
		entries, err := ts.FindKeys(keys...)
		if err != nil {
			return err
		}
		for key, id := range entries {
			merged[key] = id
		}
	}

	// check that all expected keys present in node
	if len(merged) != len(keys) {
		msg := fmt.Sprintf("entries in node %s: %d. keys inserted: %d",
			node, len(merged), len(keys))
		return errors.Wrap(errOpRetriable, msg)
	}
	for _, k := range keys {
		if _, ok := merged[k]; !ok {
			msg := fmt.Sprintf("Key '%s' not present in node %s", k, node)
			return errors.Wrap(errOpRetriable, msg)
		}
	}

	return nil
}

func genKeys(count, maxTries int, keyToNode func(string) string, filterOut []string) []string {
	exclusionSet := make(map[string]struct{})
	for _, n := range filterOut {
		exclusionSet[n] = struct{}{}
	}
	keys := make([]string, 0, count)
	i := 0
	for {
		if i >= maxTries {
			break
		}
		key := fmt.Sprintf("key-%d", i)
		i++
		// get primary node for this key
		node := keyToNode(key)
		// check if we should exclude this key
		if _, ok := exclusionSet[node]; ok {
			continue
		}
		// add key
		keys = append(keys, key)
		if len(keys) >= count {
			return keys
		}
	}
	return keys
}

func TestPauseReplica(t *testing.T) {
	if os.Getenv("ENABLE_PILOSA_CLUSTER_TESTS") != "1" {
		t.Skip("pilosa cluster tests for replication when a replica is paused are not enabled")
	}
	// configurations for test
	nodeNames := []string{"pilosa1", "pilosa2", "pilosa3"}
	nodeToPause := "pilosa3"
	addresses := make([]string, len(nodeNames))
	for i, node := range nodeNames {
		addresses[i] = getAddress(node)
	}
	clients, err := getClients(addresses)
	if err != nil {
		t.Fatalf("on init clients from addresses: %v, %v", addresses, err)
	}
	cli := clients[0]
	uris, err := getURIsFromAddresses(addresses)
	if err != nil {
		t.Fatalf("on init clients from addresses: %v, %v", addresses, err)
	}
	uri := uris[0]

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	t.Log("start Client")

	// first achieve normal cluster status
	waitForStatus(t, cli.Status, string(disco.ClusterStateNormal), 30, 1*time.Second)

	// create keyed index
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	index := fmt.Sprintf("keyed-index-%d", rng.Int63())
	err = cli.EnsureIndex(ctx, index, pilosa.IndexOptions{
		Keys: true,
	})
	if err != nil {
		t.Fatalf("creating/asserting index: %v", err)
	}

	// generate mapping from partition to primary node
	partitionToNode := make([]string, topology.DefaultPartitionN)
	for partition := 0; partition < topology.DefaultPartitionN; partition++ {
		nodes, err := cli.PartitionNodes(ctx, partition)
		if err != nil {
			t.Fatal(err)
		}
		partitionToNode[partition] = nodes[0].URI.Host
	}

	// generate random keys to insert
	// no guarantee that all keys expected to be generated don't fall
	// in nodes to be filtered out
	keyCount := 100
	maxKeyGenTries := 1000
	filterOutKeysFromTheseNodes := []string{nodeToPause}
	keyToNode := func(key string) string {
		// get partition for this key
		h := fnv.New64a()
		_, _ = h.Write([]byte(index))
		_, _ = h.Write([]byte(key))
		partition := int(h.Sum64() % uint64(topology.DefaultPartitionN))
		// get node for this partition
		return partitionToNode[partition]
	}
	keys := genKeys(keyCount, maxKeyGenTries, keyToNode, filterOutKeysFromTheseNodes)

	// pause node
	t.Logf("pause %s", nodeToPause)
	err = pauseNode(nodeToPause)
	if err != nil {
		t.Fatalf("error on pause node %s: %v", nodeToPause, err)
	}

	// insert keys
	t.Log("start insert")
	ts, err := keyInserter{
		client: cli,
		uri:    uri,
		index:  index,
		keys:   keys,
	}.insertKeys(ctx)
	if err != nil {
		t.Fatalf("Error: inserting index keys for index(%s) send to node(%s): %v", index, uri.String(), err)
	}
	t.Logf("successfully end insert: %v", len(ts))

	// wait for cluster status to be non-normal
	waitForStatus(t, cli.Status, string(disco.ClusterStateDegraded), 30, 1*time.Second)

	// wait for cluster status to get back to normal
	t.Logf("unpause %s", nodeToPause)
	err = unpauseNode(nodeToPause)
	if err != nil {
		t.Fatalf("error on unpause node %s: %v", nodeToPause, err)
	}
	waitForStatus(t, cli.Status, string(disco.ClusterStateNormal), 30, 1*time.Second)

	// set up directory to store keys
	basePath := "."
	keysDirName := "index_keys"
	dirPath, err := filepath.Abs(basePath)
	if err != nil {
		t.Fatal(err)
	}
	dirPath = filepath.Join(dirPath, keysDirName)
	err = os.Mkdir(dirPath, 0755)
	if err != nil {
		t.Fatal(err)
	}

	maxRetries := 10
	durationInBetweenRetries := 5 * time.Second
	for _, node := range nodeNames {
		try := 1
	retries:
		for {
			err = verifyNodeHasGivenKeys(ctx, node, index, dirPath, keys)
			if err == nil {
				break retries
			}
			if try <= maxRetries && errors.Is(err, errOpRetriable) {
				try++
				t.Logf("node %s, retry verify key replication: (%d/%d) after %v\n",
					node, try, maxRetries, durationInBetweenRetries)
				time.Sleep(durationInBetweenRetries)
				continue
			}
			t.Fatal(errors.Wrap(err, fmt.Sprintf("try: (%d/%d)", try, maxRetries)))
		}
	}

	cancel()
	t.Log("Done")
}
