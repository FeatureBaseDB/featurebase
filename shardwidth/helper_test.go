// Copyright 2021 Molecula Corp. All rights reserved.
package shardwidth_test

import (
	"math/rand"
	"testing"

	"github.com/molecula/featurebase/v2/shardwidth"
)

type nextShardTestCase struct {
	name         string
	haystack     [][2]uint64 // stored as shard, offset pairs
	shardIndexes []int
}

var nextShardTestCases = []nextShardTestCase{
	{
		name: "all-in-one",
		haystack: [][2]uint64{
			{0, 0},
			{0, 1},
		},
		shardIndexes: []int{2},
	},
	{
		name: "split",
		haystack: [][2]uint64{
			{0, 0},
			{1, 1},
		},
		shardIndexes: []int{1, 2},
	},
	{
		name: "two-and-one",
		haystack: [][2]uint64{
			{0, 0},
			{0, 1},
			{1, 1},
		},
		shardIndexes: []int{2, 3},
	},
}

func TestFindShards(t *testing.T) {
	for _, c := range nextShardTestCases {
		haystack := make([]uint64, len(c.haystack))
		for i, h := range c.haystack {
			haystack[i] = (h[0] << shardwidth.Exponent) + h[1]
		}
		_, indexes := shardwidth.FindShards(haystack)
		if len(indexes) != len(c.shardIndexes) {
			t.Fatalf("%s: expected %d, got %d", c.name, c.shardIndexes, indexes)
		}
		for i, expected := range c.shardIndexes {
			if indexes[i] != expected {
				t.Fatalf("%s: expected index %d to be %d, got %d", c.name, i, expected, indexes[i])
			}
		}
	}
	// fake up some more test cases
	for i := 0; i < 100; i++ {
		haystack := make([]uint64, 100)
		shard := uint64(0)
		bit := uint64(0)
		shardIndexes := []int{}
		for j := range haystack {
			if rand.Intn(30) == 0 {
				if j > 0 {
					shardIndexes = append(shardIndexes, j)
				}
				shard++
				bit = 0
			} else {
				bit += uint64(rand.Intn(30))
			}
			haystack[j] = (shard << shardwidth.Exponent) + bit
		}
		shardIndexes = append(shardIndexes, len(haystack))
		_, indexes := shardwidth.FindShards(haystack)
		if len(indexes) != len(shardIndexes) {
			t.Fatalf("trial %d: expected %d, got %d", i, shardIndexes, indexes)
		}
		for idx, expected := range shardIndexes {
			if indexes[idx] != expected {
				t.Fatalf("trial %d: expected index %d to be %d, got %d", i, idx, expected, indexes[idx])
			}
		}
	}
}
