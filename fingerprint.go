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

package pilosa

import (
	"fmt"
	"path/filepath"
	"sort"
)

type FingerPrinter struct {
	holder *Holder
}

func NewFingerPrinter(path string) *FingerPrinter {
	holder := NewHolder()
	holder.Path = path
	holder.translateFile.Path = filepath.Join(path, ".keys")
	fmt.Println("Loading")

	if err := holder.Open(); err != nil {
		fmt.Println("Problems", err)
		return nil

	}
	return &FingerPrinter{holder}
}

func calcStats(sample map[uint64]uint64) {
	if len(sample) < 1 {
		fmt.Println("no fragments")
		fmt.Println("")
		return
	}
	fmt.Println("- Total Number of Rows:", len(sample))
	pl := make(PairList, len(sample))
	i := 0
	m := make(map[uint64]uint64)
	total := uint64(0)
	for k, v := range sample {
		total += v
		pl[i] = pair{k, v}
		m[v] = m[v] + 1
		i++
	}
	sort.Sort(pl)
	fmt.Println("- Min Row Count:", pl[0].Value)
	fmt.Println("- Max Row Count:", pl[len(pl)-1].Value)
	fmt.Println("- Median Row Count:", pl[len(pl)/2].Value)
	fmt.Println("- Mean Row Count:", total/uint64(i))

	i = 0
	for k, v := range m {
		pl[i] = pair{k, v}
		i++
	}
	sort.Sort(pl)
	fmt.Println("- Mode Row Count:", pl[len(pl)-1].Key)
	fmt.Println("")
}

type pair struct {
	Key   uint64
	Value uint64
}

type PairList []pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (fp *FingerPrinter) GatherData() {
	totalArrayBits := 0
	totalBitmapBits := 0
	totalRunBits := 0
	countArrays := 0
	countBitmaps := 0
	countRuns := 0
	totalBits := uint64(0)
	totalBytes := uint64(0)
	numContainers := 0
	numFragments := 0
	fmt.Println("GO")
	for _, index := range fp.holder.indexes {
		fmt.Println("* Index:", index.Name())
		for _, field := range index.fields {
			rowSet := make(map[uint64]uint64)
			for _, view := range field.viewMap {
				for _, fragment := range view.fragments {
					info := fragment.storage.Info()
					numFragments++
					for _, containerInfo := range info.Containers {
						numContainers++
						switch containerInfo.Type {
						case "array":
							totalArrayBits += int(containerInfo.N)
							countArrays++
						case "bitmap":
							totalBitmapBits += int(containerInfo.N)
							countBitmaps++
						case "run":
							totalRunBits += int(containerInfo.N)
							countRuns++
						default:
							//unknown and corrupt

						}
						totalBits += uint64(containerInfo.N)
						totalBytes += uint64(containerInfo.Alloc)

					}
					for _, row := range fragment.rows(0) {
						rowSet[row] = rowSet[row] + fragment.storage.CountRange(row*ShardWidth, (row+1)*ShardWidth)
					}

				}
			}
			fmt.Println("** Field:", field.Name())
			calcStats(rowSet)
		}
	}
	fmt.Println("* Global for Host")
	fmt.Println("- Total ArrayBits:", totalArrayBits)
	fmt.Println("- Total BitmapBits:", totalBitmapBits)
	fmt.Println("- Total RunBits:", totalRunBits)
	fmt.Println("- Number Array Containers:", countArrays)
	fmt.Println("- Number Bitmap Conatiners:", countBitmaps)
	fmt.Println("- Number Run Containers:", countRuns)

	fmt.Println("- Mean Array Bits per Containers:", totalArrayBits/countArrays)
	fmt.Println("- Mean Bitmap Bits per Conatiners:", totalBitmapBits/countBitmaps)
	fmt.Println("- Mean Run Bits per Containers:", totalRunBits/countRuns)

	fmt.Println("- Mean Bits Per Container:", totalBits/uint64(numContainers))

	fmt.Println("- Total Bits:", totalBits)
	fmt.Println("- Total Bytes:", totalBytes)
	fmt.Println("- Total Number of Fragments:", numFragments)

}
