// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/molecula/featurebase/v3/roaring"
)

var pattern = regexp.MustCompile(`^BenchmarkCtOps/([^/]+)/([^/]+)/([^-]+)-([0-9]+)\s*([0-9]+)\s*([0-9.]+) ns/op`)

func parseFile(path string, benchmarks map[string]map[string]map[string]float64, known map[string]bool) error {
	// unset all the seen flags in the known map. if we end up with any unseen, the
	// benchmarks are incomplete.
	for k := range known {
		known[k] = false
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "BenchmarkCtOps/") {
			continue
		}
		matches := pattern.FindStringSubmatch(line)
		if matches == nil {
			return fmt.Errorf("can't parse line: '%s'", line)
		}
		t1, t2 := matches[1], matches[2]
		if _, ok := known[t1]; !ok {
			return fmt.Errorf("unknown archetype '%s'", t1)
		}
		if _, ok := known[t2]; !ok {
			return fmt.Errorf("unknown archetype '%s'", t2)
		}
		known[t1] = true
		known[t2] = true
		op := matches[3]
		time, err := strconv.ParseFloat(matches[6], 64)
		if err != nil {
			return fmt.Errorf("parsing float [%s]: %v", matches[6], err)
		}
		if benchmarks[op] == nil {
			benchmarks[op] = make(map[string]map[string]float64, 16)
		}
		if benchmarks[op][t1] == nil {
			benchmarks[op][t1] = make(map[string]float64, 16)
		}
		benchmarks[op][t1][t2] += time
	}
	for k, v := range known {
		if !v {
			fmt.Printf("warning: container archetype '%s' missing in benchmarks\n", k)
		}
	}
	return nil
}

func main() {
	maxLen := 0

	knownArchetypes := make(map[string]bool, len(roaring.ContainerArchetypeNames))
	for _, name := range roaring.ContainerArchetypeNames {
		if len(name) > maxLen {
			maxLen = len(name)
		}
		knownArchetypes[name] = false
	}
	benchmarks := make(map[string]map[string]map[string]float64, 8)
	for _, file := range os.Args[1:] {
		err := parseFile(file, benchmarks, knownArchetypes)
		if err != nil {
			log.Fatalf("parsing '%s': %v", file, err)
		}
	}
	if len(benchmarks) < 1 {
		log.Fatalf("no benchmarks parsed?")
	}
	ops := make([]string, 0, 8)
	for k := range benchmarks {
		ops = append(ops, k)
	}
	sort.Strings(ops)
	for _, op := range ops {
		fmt.Printf("%s:\n", op)
		fmt.Printf("%*s ", maxLen, "")
		for _, name := range roaring.ContainerArchetypeNames {
			fmt.Printf(" %*s", maxLen, name)
		}
		fmt.Print("\n")
		for _, self := range roaring.ContainerArchetypeNames {
			fmt.Printf("%*s ", maxLen, self)
			for _, other := range roaring.ContainerArchetypeNames {
				time, ok := benchmarks[op][self][other]
				if ok {
					fmt.Printf(" %*.1f", maxLen, time)
				} else {
					fmt.Printf(" %*s", maxLen, "--")
				}
			}
			fmt.Print("\n")
		}
		fmt.Print("\n")
	}
}
