package partitioner_test

import (
	"fmt"
	"testing"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/mds/controller/partitioner"
	"github.com/stretchr/testify/assert"
)

func TestPartitioner(t *testing.T) {
	tableKey := dax.TableKey("foo")
	partitionN := 8

	t.Run("PartitionForKeys", func(t *testing.T) {
		p := partitioner.NewPartitioner()

		tests := []struct {
			tkey       dax.TableKey
			partitionN int
			keys       []string
			exp        map[dax.PartitionNum][]string
		}{
			{
				tkey:       tableKey,
				partitionN: partitionN,
				keys:       []string{"a"},
				exp: map[dax.PartitionNum][]string{
					2: {"a"},
				},
			},
			{
				tkey:       tableKey,
				partitionN: partitionN,
				keys:       []string{"a", "a"},
				exp: map[dax.PartitionNum][]string{
					2: {"a", "a"},
				},
			},
			{
				tkey:       "differentTableName",
				partitionN: partitionN,
				keys:       []string{"a"},
				exp: map[dax.PartitionNum][]string{
					4: {"a"},
				},
			},
			{
				tkey:       tableKey,
				partitionN: partitionN,
				keys:       []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"},
				exp: map[dax.PartitionNum][]string{
					0: {"g"},
					1: {"d"},
					2: {"a", "i"},
					3: {"f"},
					4: {"c"},
					5: {"h"},
					6: {"e"},
					7: {"b"},
				},
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				out := p.PartitionsForKeys(test.tkey, test.partitionN, test.keys...)

				assert.ElementsMatch(t, keys(test.exp), keys(out))

				for k := range out {
					assert.ElementsMatch(t, test.exp[k], out[k])
				}
			})
		}

	})
}

func keys(m map[dax.PartitionNum][]string) dax.PartitionNums {
	keys := make(dax.PartitionNums, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
