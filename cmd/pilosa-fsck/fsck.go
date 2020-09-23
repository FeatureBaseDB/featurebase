// Copyright 2020 Pilosa Corp.
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

package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/boltdb"
	"github.com/pilosa/pilosa/v2/internal"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pkg/errors"
	"github.com/zeebo/blake3"
)

// pilosa-fsck :
// an external customer tool (originally for Q2) to do 2 jobs:
// Given a set of cluster backups (and their .id and .topology files)
// mounted on the same file system, we can:
// 1) scan for fragment differences between the primary and its replicas (default); or
// 2) repair those differences by overwriting the replcas with the primary fragments (if -fix is given).
//
// pilosa-chk is deliberately NOT a part of pilosa so that it can run without
// forcing a customer to upgrade or downgrade their installed version.

// FsckConfig configures the dumpcols() and/or read() runs.
type FsckConfig struct {
	Fix    bool // -fix
	FixCol bool // -fixcol

	Colkeydump bool // -col

	// -col column key dump only options:
	Dir         string
	Index       string
	PartitionID int
	ShowHeader  bool
	ShowKey     bool
	ShowID      bool

	// not flags, just the Args() left after all other flags. Should be the list
	// of pilosa (holder) directories for the cluster.
	Dirs []string

	Verbose bool // -v
	Quiet   bool // -q

	// manual workaround for not having PilosaConfigPath, if really need be.
	ReplicaN         int    // -replicas
	PilosaConfigPath string // -config

	topo *pilosa.Topology
}

// call DefineFlags before myflags.Parse()
func (cfg *FsckConfig) DefineFlags(fs *flag.FlagSet) {
	fs.BoolVar(&cfg.Fix, "fix", false, "(warning: alters the backed-up node images on disk) copy primary data to replicas to create a consistent cluster. Implies -fixcol")
	fs.BoolVar(&cfg.FixCol, "fixcol", false, "(warning: alters the backed-up node images on disk) repair string key translation tables. Skip repair of index data.")
	fs.BoolVar(&cfg.Verbose, "v", false, "be very verbose during analysis")
	fs.BoolVar(&cfg.Quiet, "q", false, "be very quiet")

	fs.IntVar(&cfg.ReplicaN, "replicas", 0, "(required) manually entered replicaN; the number of replicas maintained in the cluster. Must be the same as the [cluster] 'replicas = R' entry in the pilosa.conf file for the cluster.")

	fs.StringVar(&cfg.PilosaConfigPath, "config", "", "(required: -replicas or -config, with -config preferred) path to the pilosa.conf for the cluster (e.g. /etc/pilosa.conf)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "pilosa-fsck version: %v\n\n", pilosa.VersionInfo())
		fmt.Fprintf(os.Stderr, `Use: pilosa-fsck -replicas R {-fix} {-fixcol} {-q} {-v} /backup/1/.pilosa /backup/2/.pilosa ... /backup/N/.pilosa

  -fix
    	(warning: alters the backed-up node images on disk) copy primary data to replicas to create a consistent cluster.  Implies -fixcol

  -fixcol
        (warning: alters the backed-up node images on disk) repair string key translation tables. Skip repair of index data.

  -replicas R 
        (required) R is a positive integer, giving the replicaN or replicator factor for the cluster. This is
        the number of replicas maintained in the cluster. Must be the same as the 
        [cluster] 'replicas = R' entry shared across all the pilosa.conf files on each node.

  -v
        be very verbose during analysis
  -q
        be very quiet during analysis and repair

`)
		/*
			key translation dump options, usually only used by developers debugging key translation:

			  -col
			    	(optional) display column keys (very long output)
			  -dir string
			    	(optional; requires -col), one pilosa data dir to read (default "/home/ubuntu/.pilosa")
			  -header
			    	(optional; requires -col), display header
			  -id
			    	(optional; requires -col), dump reverse mapping id->key
			  -index string
			    	(optional; requires -col), index name (default "i")
			  -key
			    	(optional; requires -col), dump forward mapping key->id
			  -partition int
			    	(optional; requires -col), partition id to dump

		*/
		fmt.Fprintf(os.Stderr, `
Welcome to pilosa-fsck. This is a scan and repair 
tool that is modeled after the classic unix file 
system utility fsck. 

WARNING: DO NOT RUN ON A LIVE SYSTEM.

The most important point to remember is that analysis
and repair must be done *offline*.

Just as fsck must be run on an unmounted disk, 
pilosa-fsck must be run on a backup. It must 
not be run on the directories where a live Pilosa system
is serving queries. Instead, take a backup first.
A backup is a set of N cluster-node directories that have been
copied from your live system. They must all 
be visible and mounted on one filesystem together.

pilosa-fsck can be run in scan-mode (without -fix or -fixcol),
or in repair-mode with -fix (or -fixcol). The console output
supplies a shell script documenting the analysis 
and showing what data changes would be made. If a
fix has been requested, those fixes will have
been applied during the run. The output then serves
as documentation of what has been updated. If
a fix has not been requested (in other words, neither
-fix nor -fixcol was given) then no changes will
have been made to the backups. The fragment level
sync can be completed next by running the script
if you wish. The -fixcol fixes can only be
applied by doing a -fixcol run of pilosa-fsck.

REQUIRED COMMAND LINE ARGUMENTS

The paths to all the top-level pilosa 
directories in a cluster must be given on the command
line. The -replicas R flag is also always required. It
must be correct for your cluser. Here R is the same as
the [cluster] stanza "replicas = R" line from your
pilosa.conf.

Example:

Suppose you are ready to run pilosa-fsck: 
you have taken a backup of your four node Pilosa 
cluster and stored it all on one filesystem with 
all nodes visible and uncompressed. This
is a pre-requisite to running pilosa-fsck. 
Let's suppose we have replication R = 3 set.
In this example, have stored our backed-up directories in 

/backup/molecula

and the four node backups are in 
subdirectories node1/ node2/ node3/ node4/ under this:

/backup/molecula/node1/
/backup/molecula/node1/.pilosa/.id
/backup/molecula/node1/.pilosa/.topology
/backup/molecula/node1/.pilosa/myindex

/backup/molecula/node2/
/backup/molecula/node2/.pilosa/.id
/backup/molecula/node2/.pilosa/.topology
/backup/molecula/node2/.pilosa/myindex

/backup/molecula/node3/
/backup/molecula/node3/.pilosa/.id
/backup/molecula/node3/.pilosa/.topology
/backup/molecula/node3/.pilosa/myindex

/backup/molecula/node4/
/backup/molecula/node4/.pilosa/.id
/backup/molecula/node4/.pilosa/.topology
/backup/molecula/node4/.pilosa/myindex

NOTE: your .pilosa directories need not be named .pilosa. They can
be something else, such as when the -d flag to pilosa server was used.
The .id and .topology and index directories must be found directly underneath.

Then a typical invocation to scan a cluster backup for issues:

$ cd /backup/molecula/
$ pilosa-fsck -replicas 3 node1/.pilosa node2/.pilosa node3/.pilosa node4/.pilosa

A typical invocation to repair the replication in the same backup:

$ pilosa-fsck -replicas 3 -fix node1/.pilosa node2/.pilosa node3/.pilosa node4/.pilosa

In both cases, the .id and .topology files must 
be present in the backups.

KEY REPAIR NOTE

While the output of pilosa-fsck wihtout -fix or -fixcol 
gives a script showing the index fragment repair operations 
that can be applied by (cp/rm) shell commands subsequently, 
this script alone is an incomplete repair. It does not
address string key tranlsation repairs. For a complete repair, 
a run of pilosa-fsck with the -fixcol or -fix flags 
will be required.

A note about the -fixcol key translation repairs: these are fine
grained operations on the internal databases that do not have
corresponding (cp/rm) shell commands. Therefore a run of pilosa-fsck
with the -fix or -fixcol flag is required to repair these.
`)
	}
}

// call c.ValidateConfig() after myflags.Parse()
func (c *FsckConfig) ValidateConfig() error {
	if c.Fix {
		c.FixCol = true
	}
	if c.ReplicaN == 0 && c.PilosaConfigPath == "" {
		return fmt.Errorf("must supply -replicas with the replica count from your pilosa.conf (positive integer count)")
	}

	if c.ReplicaN == 0 && c.PilosaConfigPath != "" {

		if !FileExists(c.PilosaConfigPath) {
			return fmt.Errorf(" -config path '%v' does not exist", c.PilosaConfigPath)
		}
		by, err := ioutil.ReadFile(c.PilosaConfigPath)
		if err != nil {
			return fmt.Errorf("error: could not read the -config path '%v': '%v'", c.PilosaConfigPath, err)
		}
		srvcfg, err := server.ParseConfig(string(by))
		if err != nil {
			//vv("warning: -config path '%v' problem, could not parse toml: '%v'", c.PilosaConfigPath, err)

			// fall back to manual parsing of config
			lines := strings.Split(string(by), "\n")
			clusterStart := -1
			for i, line := range lines {
				if strings.Contains(line, `[cluster]`) {
					clusterStart = i
				}
				if i > clusterStart {
					if strings.Contains(line, "replicas") {
						split := strings.Split(line, "=")
						ns := strings.TrimSpace(split[1])
						n, err := strconv.Atoi(ns)
						if err != nil {
							return fmt.Errorf("error: could not parse the replicaN from line %v in -config path '%v' (%v): '%v'", i+1, c.PilosaConfigPath, line, err)
						}
						c.ReplicaN = n
					}
				}
			}
		} else {
			c.ReplicaN = srvcfg.Cluster.ReplicaN
		}
		if c.ReplicaN == 0 {
			return fmt.Errorf("error: -config path '%v' did not list the Replica count: cannot be 0. See the [cluster] section, the 'replicas = R' line.", c.PilosaConfigPath)
		}
		//vv("c.ReplicaN = %v", c.ReplicaN)
	}
	return nil
}

var ProgramName = "pilosa-fsck"

func main() {

	myflags := flag.NewFlagSet(ProgramName, flag.ContinueOnError)
	cfg := &FsckConfig{}
	cfg.DefineFlags(myflags)

	err := myflags.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n%v\n", err.Error())
		os.Exit(1)
	}
	err = cfg.ValidateConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s error: %s\n", ProgramName, err)
		os.Exit(1)
	}
	dirs := myflags.Args()
	nDir := len(dirs)
	if nDir <= 0 && !cfg.Colkeydump {
		fmt.Fprintf(os.Stderr, "error: %v command line arguments missing error: provide all of the top-level pilosa directories for the cluster as command line arguments.\n", ProgramName)
		os.Exit(1)
	}

	cmdline := strings.Join(os.Args, " ")

	// make sure all the dir are distinct
	dup := make(map[string]bool)
	for _, dir := range dirs {
		if dup[dir] {
			fmt.Fprintf(os.Stderr, "%v error: duplicate data directory '%v' given in command line '%v'. Each backup directory must be distinct.\n", ProgramName, dir, cmdline)
			os.Exit(1)
		} else {
			dup[dir] = true
		}
	}

	fmt.Fprintf(os.Stdout, "#!/bin/bash\n\n# pilosa-fsck version: %v\n", pilosa.VersionInfo())
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: could not read current dir: '%v'\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "# cwd: %v\n", cwd)
	fmt.Fprintf(os.Stdout, "# command line: %v\n", cmdline)
	t0 := time.Now()
	fmt.Fprintf(os.Stdout, "# started at %v\n\n", t0.Format(RFC3339MsecTz0))
	defer func() {
		fmt.Fprintf(os.Stdout, "# finished at %v (elapsed %v)\n\n", time.Now().Format(RFC3339MsecTz0), time.Since(t0))
	}()
	cfg.Dirs = dirs

	_, err = cfg.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func (cfg *FsckConfig) Run() (fixNeeded bool, err error) {

	if cfg.Colkeydump {
		cfg.dumpcols()
	}

	perNodeIndexMaps, clusterNodes, ats, err := cfg.read()
	if err != nil {
		return false, err
	}

	if cfg.FixCol {
		err := cfg.RepairTranslationStores(ats)
		if err != nil {
			return false, fmt.Errorf("error fixing key translation stores with cfg.RepairTranslationStores(): '%v'\n", err)
		}
	}

	//vv("perNodeIndexMaps='%#v', clusterNodes='%#v'", perNodeIndexMaps, clusterNodes)

	fixme, reports, err := cfg.analyze(clusterNodes, perNodeIndexMaps, ats)
	if err != nil {
		return false, fmt.Errorf("error in FsckConfig.analyze(): '%v'", err)
	}
	fixNeeded = ats.RepairNeeded || fixme
	for _, report := range reports {
		fmt.Printf("%v\n", report)
	}
	if len(reports) == 0 {
		fmt.Fprintf(os.Stderr, "pilosa-fsck: no index found to analyze. cmdline was: %v\n", strings.Join(os.Args, " "))
	}
	return
}

var _ = (&FsckConfig{}).dumpAts

func (cfg *FsckConfig) dumpAts(ats *pilosa.AllTranslatorSummary) {
	fmt.Printf("# dumpAts:  	RepairNeeded=%v\n", ats.RepairNeeded)
	for _, sum := range ats.Sums {
		fmt.Printf("# sum = '%#v'\n", sum)
	}

}

type group struct {
	elem        []*pilosa.TranslatorSummary
	partitionID int
}

func (g *group) String() (s string) {
	for i, e := range g.elem {
		s += fmt.Sprintf("partition %v, group elem [%v] out of %v: %v\n", g.partitionID, i, len(g.elem), e.String())
	}
	return
}

func (cfg *FsckConfig) RepairTranslationStores(ats *pilosa.AllTranslatorSummary) (err error) {

	verbose := cfg.Verbose

	m := make(map[int]*group)
	for _, sum := range ats.Sums {
		if !sum.IsColKey {
			continue
		}
		grp := m[sum.PartitionID]
		if grp == nil {
			grp = &group{
				partitionID: sum.PartitionID,
			}
			m[sum.PartitionID] = grp
		}
		grp.elem = append(grp.elem, sum)
	}

	for partitionID, group := range m {
		_ = partitionID
		prim := -1
		keyCount := 0
		for k, e := range group.elem {
			if e.IsPrimary {
				prim = k
			}
			keyCount += e.KeyCount
		}
		if prim == -1 {
			panic(fmt.Sprintf("no primary found for group '%v'", group.String()))
		}

		primary := group.elem[prim]
		primaryChecksum := primary.Checksum
		for _, e := range group.elem {
			if e.IsPrimary {
				continue
			}
			// is e a replica? not necessarily! have to check.
			if !e.IsReplica {
				//if verbose {
				// since this will happen even on a fix point, where it is already empty,
				// we don't report it again.
				//fmt.Printf("# non-replica should have no data: creating an empty translation store here at '%v'\n", e.StorePath)
				//}
				err := os.RemoveAll(e.StorePath)
				if err != nil {
					return errors.Wrap(err, fmt.Sprintf("RepairTranslationStores() os.RemoveAll(e.StorePath='%v')", e.StorePath))
				}
				store, err := boltdb.OpenTranslateStore(e.StorePath, e.Index, e.Field, e.PartitionID, pilosa.DefaultPartitionN)
				if err != nil {
					return errors.Wrap(err, fmt.Sprintf("RepairTranslationStores() create empty boldtdb: boltdb.OpenTranslateStore e.StorePath='%v'", e.StorePath))
				}
				err = store.Close()
				if err != nil {
					return errors.Wrap(err, fmt.Sprintf("RepairTranslationStores() closing empty boltdb at path '%v'", e.StorePath))
				}
				continue
			}
			// INVAR: e is a replica for this paritionID.
			// Copy from primary if checksums are different.
			if e.Checksum != primaryChecksum {
				from := group.elem[prim].StorePath
				dest := e.StorePath
				if verbose {
					fmt.Printf("# e.Checksum '%v' != primaryChecksum '%v': copying from primary translation store '%v' -> '%v'\n", e.Checksum, primaryChecksum, from, dest)
				}
				err := cp(from, dest)
				if err != nil {
					return fmt.Errorf("error: could not copy from primary '%v' to replica translation store '%v': '%v' ... try to keep going...\n", from, dest, err)
				}
			}
		}
	}
	return nil
}

func (cfg *FsckConfig) dumpcols() {

	verbose := cfg.Verbose
	quiet := cfg.Quiet
	_, _ = verbose, quiet

	dir := cfg.Dir
	index := cfg.Index
	partitionID := cfg.PartitionID
	showKey := cfg.ShowKey
	showID := cfg.ShowID

	if !quiet {
		fmt.Printf("# dumpcols: opening dir '%v'... this may take a few minutes...\n", dir)
	}
	holder := pilosa.NewHolder(dir, nil)
	holder.OpenTranslateStore = boltdb.OpenTranslateStore
	err := holder.Open()
	if err != nil {
		log.Fatal(err)
	}
	if cfg.ShowHeader {
		fmt.Println("# columnKey columId")
	}
	id_key := make(map[uint64]string)
	key_id := make(map[string]uint64)
	for _, idx := range holder.Indexes() {
		fmt.Printf("# Looking '%v'\n", idx.Name())
		if idx.Name() == index {
			store := idx.TranslateStore(partitionID)
			fmt.Printf("# Key By ID partitionID = %v\n", partitionID)
			err := store.KeyWalker(func(key string, col uint64) {
				key_id[key] = col
				if showKey {
					fmt.Printf("# '%v' %v shard: %v partition: %v\n", key, col, col/pilosa.ShardWidth, partitionID)
				}
			})
			panicOn(err)
		}
	}
	for _, idx := range holder.Indexes() {
		if idx.Name() == index {
			store := idx.TranslateStore(partitionID)
			//fmt.Printf("# ID ByKey\n")
			err := store.IDWalker(func(key string, col uint64) {
				id_key[col] = key
				if showID {
					fmt.Printf("# '%v' %v\n", key, col)
				}
			})
			panicOn(err)
		}
	}
	fmt.Printf("# k: %d i: %d\n", len(key_id), len(id_key))
	fmt.Println("id_key")
	for k, v := range id_key {
		l, ok := key_id[v]
		if ok {
			if k != l {
				fmt.Printf("# X: %v %v %v\n", k, l, v)
			}
		} else {
			fmt.Printf("# key not in id %v\n", v)
		}
	}
	fmt.Println("key_id")
	for k, v := range key_id {
		l, ok := id_key[v]
		if ok {
			if k != l {
				fmt.Printf("# T: %v %v %v\n", k, l, v)
			}
		} else {
			fmt.Printf("# id not in key %v\n", v)
		}
	}
}

func (cfg *FsckConfig) read() (perNodeIndexMaps []map[string]*pilosa.IndexFragmentSummary, clusterNodes []string, final *pilosa.AllTranslatorSummary, err error) {

	final = pilosa.NewAllTranslatorSummary()

	dirs := cfg.Dirs
	for _, dir := range dirs {
		idx2frag, nodeID, atsNode, err := cfg.readOneDir(dir)
		if err != nil {
			return nil, nil, nil, err
		}
		final.Append(atsNode)
		clusterNodes = append(clusterNodes, nodeID)
		perNodeIndexMaps = append(perNodeIndexMaps, idx2frag)
	}
	return
}

func (cfg *FsckConfig) readOneDir(dir string) (idx2frag map[string]*pilosa.IndexFragmentSummary, nodeID string, atsNode *pilosa.AllTranslatorSummary, err error) {

	verbose := cfg.Verbose
	quiet := cfg.Quiet

	if !quiet {
		fmt.Printf("# opening dir '%v'... this may take a few minutes... fixcol=%v\n\n", dir, cfg.FixCol)
	}

	jmphasher := &pilosa.Jmphasher{}
	partitionN := pilosa.DefaultPartitionN
	replicaN := cfg.ReplicaN
	topo, err := loadTopology(dir, jmphasher, partitionN, replicaN)
	if err != nil {
		return nil, "", nil, err
	}
	cfg.topo = topo
	//vv("topo = '%#v'", topo)
	nodeIDs := topo.GetNodeIDs()
	//vv("nodeIDs = '%#v'", nodeIDs)
	nNodes := len(nodeIDs)
	nDir := len(cfg.Dirs)
	if nDir != nNodes {
		return nil, "", nil, fmt.Errorf("command line had %v directories (%#v) but the .topology had %v nodes (%#v)", nDir, cfg.Dirs, nNodes, nodeIDs)
	}

	holder := pilosa.NewHolder(dir, nil)
	holder.OpenTranslateStore = boltdb.OpenTranslateStore

	nodeID, err = holder.LoadNodeID()
	panicOn(err)
	//vv("nodeID = '%v'", nodeID)
	err = holder.Open()

	if err != nil {
		log.Fatal(err)
	}

	if !quiet {
		fmt.Printf("\n# calculating hashes of row and column key translation maps on data from dir '%v'...\n", dir)
	}
	var indexes []*pilosa.Index

	const checkKeys = true
	atsNode = pilosa.NewAllTranslatorSummary()
	for _, idx := range holder.Indexes() {
		asum, err := idx.ComputeTranslatorSummary(verbose, checkKeys, cfg.FixCol, topo, nodeID)
		if err != nil {
			log.Fatal(err)
		}
		atsNode.Append(asum)
		indexes = append(indexes, idx)
	}
	atsNode.Sort()

	hasher := blake3.New()
	if !quiet {
		fmt.Printf("\n# summary of col/row translations in dir: %v:\n", dir)
	}
	for _, sum := range atsNode.Sums {
		if !quiet {
			fmt.Printf("# index: %v  partitionID: %v blake3-%v keyCount: %v idCount: %v\n", sum.Index, sum.PartitionID, sum.Checksum, sum.KeyCount, sum.IDCount)
		}
		_, _ = hasher.Write([]byte(sum.Checksum))
	}

	var buf [16]byte
	_, _ = hasher.Digest().Read(buf[0:])

	if !quiet {
		fmt.Printf("# all-checksum = blake3-%x\n", buf)
	}

	// fragment analysis

	showBits := false
	showOpsLog := false
	idx2frag = make(map[string]*pilosa.IndexFragmentSummary) // on this node.
	for _, idx := range indexes {
		if verbose {
			fmt.Printf("# ==============================\n")
			fmt.Printf("# index: %v\n", idx.Name())
			fmt.Printf("# ==============================\n")
		}
		frgsum := idx.WriteFragmentChecksums(os.Stdout, showBits, showOpsLog, topo, verbose)
		frgsum.Dir = dir
		frgsum.NodeID = nodeID
		idx2frag[idx.Name()] = frgsum
	}

	_ = holder.Close()

	//vv("idx2frag = '%v'", idx2frag) // tons of output. see 1234.out.full for examaple.

	return
}

// from cluster.go:1924
func loadTopology(holderDir string, hasher pilosa.Hasher, partitionN, replicaN int) (*pilosa.Topology, error) {

	buf, err := ioutil.ReadFile(filepath.Join(holderDir, ".topology"))
	if err != nil {
		return nil, err
	}

	var pb internal.Topology
	err = proto.Unmarshal(buf, &pb)
	if err != nil {
		return nil, err
	}

	return pilosa.DecodeTopology(&pb, hasher, partitionN, replicaN, nil)
}

func (cfg *FsckConfig) analyze(clusterNodes []string, perNodeIndexMaps []map[string]*pilosa.IndexFragmentSummary, ats *pilosa.AllTranslatorSummary) (fixNeeded bool, reports []string, err error) {

	verbose := cfg.Verbose
	quiet := cfg.Quiet
	_, _ = verbose, quiet

	allIndex := make(map[string]bool)
	for _, mp := range perNodeIndexMaps {
		for index := range mp {
			allIndex[index] = true
		}
	}
	if !quiet {
		vv("allIndex = '%#v'", allIndex)
	}
	for index := range allIndex {
		if !quiet {
			vv("on index '%v'", index)
		}
		nodes2fragsum := make(map[string]*pilosa.IndexFragmentSummary)
		for _, mp := range perNodeIndexMaps {
			sum := mp[index]
			if sum == nil {
				continue
			}
			nodes2fragsum[sum.NodeID] = sum
		}
		fixme, report, err := cfg.analyzeThisIndex(index, nodes2fragsum, ats)
		if err != nil {
			return false, reports, fmt.Errorf("error in analyze of index '%v': '%v'", index, err)
		}
		fixNeeded = fixNeeded || fixme
		reports = append(reports, report)
	}
	return fixNeeded, reports, nil
}

func (cfg *FsckConfig) analyzeThisIndex(
	index string,
	nodes2fragsum map[string]*pilosa.IndexFragmentSummary,
	ats *pilosa.AllTranslatorSummary,
) (fixNeeded bool, report string, err error) {

	verbose := cfg.Verbose
	quiet := cfg.Quiet
	_, _ = verbose, quiet

	var removedBytes int64
	var copiedBytes int64
	var changedFiles int64
	var totalFiles int64
	var overwrittenBytes int64
	var totalBytes int64

	if !quiet {
		vv("top of analyzeThisIndex(index='%v'); len of nodes2fragsum = %v; nodes2fragsum='%#v'",
			index, len(nodes2fragsum), nodes2fragsum)
	}

	for node, sum := range nodes2fragsum {
		if !quiet {
			fmt.Printf("# on node '%v'\n", node)
		}
		// do they disagree on who is the primary?
		// for each fragment, do they disagree on the checksum?

		// Q: which nodes are supposed to have data, and which
		// nodes are not supposed to have data?

		//	loopFragSum:
		for relpath, fragsum := range sum.RelPath2fsum {
			fragsum.NodeID = node
			totalFiles++
			//vv("checking %v on node %v", relpath, node)

			replicas, nonReplicas := cfg.topo.GetReplicasForPrimary(fragsum.Primary)
			_, _ = replicas, nonReplicas
			//vv("replicas = '%#v'", replicas)
			//vv("nonReplicas = '%#v'", nonReplicas)

			err := cfg.verifyReplicasAvailable(replicas, nonReplicas, nodes2fragsum, fragsum)
			if err != nil {
				return fixNeeded, "", err
			}

			// find the primary's checksum
			primaryChecksum := ""
			var primaryFragSum *pilosa.FragSum
			for node, isPrimary := range replicas {
				if isPrimary {
					primarySum := nodes2fragsum[node]
					primaryFragSum = primarySum.RelPath2fsum[relpath]
					if primaryFragSum == nil {

						// This seems clear indication that we have the topology wrong.
						// When the topology is right, there are NO errors of this kind.
						//
						msg := fmt.Sprintf("# ugh. BAD. Stopping because any fix will be wrong. We see wrong -replica %v param, OR the .id files are mis-assigned with respect to the topology file. Could not find primary FragSum for relpath = '%v'. replicas = '%#v', nonReplicas = '%#v'\n", cfg.ReplicaN, relpath, replicas, nonReplicas)
						vv(msg)
						fmt.Fprintf(os.Stderr, "%v\n", msg)
						panic(msg) // stop. the fixes are going to be wrong.
					} else {
						primaryChecksum = primaryFragSum.Checksum
						primaryFragSum.NodeID = node
						primaryFragSum.ScanDone = true
					}
					break
				}
			}
			if primaryChecksum == "" {
				return fixNeeded, "", fmt.Errorf("could not find primary replica??? replicas='%#v', nodes2fragsum='%v'; for fragsum='%#v'", replicas, nodes2fragsum, fragsum)
			}

			// is this a non-replica?
			_, isNon := nonReplicas[fragsum.NodeID]
			if isNon {
				removedBytes += FileSize(fragsum.AbsPath)
				changedFiles++

				//vv("yes, is nonReplica: fragsum.NodeID='%v'", fragsum.NodeID)
				if !quiet {
					fmt.Printf("rm %v             #### REPAIR REMOVE data from non-replica at node '%v' (fragsum='%#v') vs. primary (%#v)\n\n", fragsum.AbsPath, node, fragsum, primaryFragSum)
				}
				if cfg.Fix {
					err := os.Remove(fragsum.AbsPath)
					if err != nil {
						return fixNeeded, "", fmt.Errorf("error removing non-replica extra fragment '%v': '%v'", fragsum.AbsPath, err)
					}
				}
			} else {
				presz := FileSize(fragsum.AbsPath)
				totalBytes += presz

				checksum := fragsum.Checksum
				if checksum != primaryChecksum {
					copiedBytes += FileSize(primaryFragSum.AbsPath)
					changedFiles++
					overwrittenBytes += presz

					if !quiet {
						fmt.Printf("cp %v %v        #### REPAIR OVERWRITE replica at node '%v' (%#v) from primary '%v' (%#v)\n", primaryFragSum.AbsPath, fragsum.AbsPath, node, fragsum, primaryFragSum.NodeID, primaryFragSum)
					}
					if cfg.Fix {
						err := cp(primaryFragSum.AbsPath, fragsum.AbsPath)
						if err != nil {
							return fixNeeded, "", fmt.Errorf("error copying from '%v' to '%v': '%v'",
								primaryFragSum.AbsPath, fragsum.AbsPath, err)
						}
					}
				}
			}
			fragsum.ScanDone = true
		}
	}
	nDir := len(nodes2fragsum)

	keyCount, idCount := cfg.getKeyIDCounts(ats)

	fixNeeded = changedFiles > 0 || ats.RepairNeeded
	var actionTaken string
	var wouldBe string
	if cfg.Fix || cfg.FixCol {
		if fixNeeded {
			actionTaken = "*REPAIRS WERE MADE TO THE BACKUPS*"
			wouldBe = "sync repairs made:"
		} else {
			wouldBe = ""
			actionTaken = "NO REPAIR NEEDED."
		}
	} else {
		if fixNeeded {
			wouldBe = "sync actions that would be taken under -fix:"
			actionTaken = "*REPAIRS NEEDED BUT WERE NOT APPLIED* ; pilosa-fsck -fix and -fixcol were omitted."
		} else {
			wouldBe = ""
			actionTaken = "NO REPAIR NEEDED."
		}
	}
	var fragUpdate string
	if changedFiles > 0 {
		fragUpdate = fmt.Sprintf(`
#  %v
#    copied bytes: %v
#    file bytes overwritten: %v
#    new bytes added: %v
#    new bytes is %0.01f%% of %v total bytes
#    removed %v bytes from non-replicas
#    changed file count %v (%0.01f%%; total files=%v)
#
`, wouldBe, humanize.Comma(copiedBytes), humanize.Comma(overwrittenBytes), humanize.Comma(copiedBytes-overwrittenBytes), 100*float64(copiedBytes-overwrittenBytes)/float64(totalBytes), humanize.Comma(totalBytes), humanize.Comma(removedBytes), changedFiles, 100*float64(changedFiles)/float64(totalFiles), humanize.Comma(totalFiles))
	}

	report = fmt.Sprintf(`
# ========================================================
#   pilosa-fsck final report 
#
#     run with    -fix: %v 
#              -fixcol: %v
#
# index examined: '%v'
#
# nodes examined: %v
#       -replicas %v replication factor used
#
# feature data examined: %v bytes
# feature files examined: %v files
#
# key-translation-stores examined: %v
#                       key-count: %v   over all replicas
#                        id-count: %v   over all replicas
#
# %v
# %v
# ========================================================
`,
		cfg.Fix, cfg.FixCol, index, nDir, cfg.ReplicaN, humanize.Comma(totalBytes), humanize.Comma(totalFiles), humanize.Comma(int64(nDir*pilosa.DefaultPartitionN)), humanize.Comma(int64(keyCount)), humanize.Comma(int64(idCount)), actionTaken, fragUpdate)
	return
}

func (cfg *FsckConfig) verifyReplicasAvailable(replicas, nonReplicas map[string]bool, nodes2fragsum map[string]*pilosa.IndexFragmentSummary, fragsum *pilosa.FragSum) error {
	for node := range replicas {
		if nodes2fragsum[node] == nil {
			return fmt.Errorf("error: node '%v' needed for a replica set was not availabe. Did you give ALL the directories for your cluster on the command line at once? In nodes2fragsum '%#v' (replicas: '%#v'; non-replicas '%#v') for fragsum '%v'", node, nodes2fragsum, replicas, nonReplicas, fragsum)
		}
	}
	return nil
}

func cp(fromPath, toPath string) (err error) {
	tmpTo := toPath + ".fsck.tmp"
	toFd, err := os.Create(tmpTo)
	if err != nil {
		return err
	}
	defer toFd.Close()
	fromFd, err := os.Open(fromPath)
	if err != nil {
		return err
	}
	defer fromFd.Close()

	_, err = io.Copy(toFd, fromFd)
	if err != nil {
		return err
	}
	err = toFd.Close()
	if err != nil {
		return err
	}
	return os.Rename(tmpTo, toPath)
}

func (cfg *FsckConfig) getKeyIDCounts(ats *pilosa.AllTranslatorSummary) (keyCount, idCount int) {
	for _, sum := range ats.Sums {
		keyCount += sum.KeyCount
		idCount += sum.IDCount
	}
	return
}
