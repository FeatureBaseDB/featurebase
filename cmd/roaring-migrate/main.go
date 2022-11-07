// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/rbf"
	"github.com/featurebasedb/featurebase/v3/rbf/cfg"
	"github.com/featurebasedb/featurebase/v3/roaring"
	txkey "github.com/featurebasedb/featurebase/v3/short_txkey"
	"github.com/featurebasedb/featurebase/v3/vprint"
	"github.com/spf13/cobra"
)

var (
	visited map[string]int64
	glogger = logger.NewStandardLogger(os.Stdout)
)

const (
	Version = "1.0"
)

func main() {
	os.Exit(realMain())
}

func realMain() int {
	visited = make(map[string]int64)
	var dataDir, backupPath string
	var verbose bool
	cmdMigrate := &cobra.Command{
		Use:   "roaring-migrate",
		Short: "convert roaring pilosa backup to rbf",
		Long:  `roaring-migrate uses the pilosa data-dir for each node, and produces a new backup that is able to be restored from utilizing the new pilosa restore tool.`,
		Run: func(cmd *cobra.Command, args []string) {
			if verbose {
				glogger.Infof("Version: %v", Version)
			}
			nodes := strings.Split(dataDir, ",")
			for _, nodePath := range nodes {
				err := Migrate(nodePath, backupPath, verbose)
				if err != nil {
					glogger.Errorf("%v", Version)
					return
				}

			}
		},
	}
	cmdMigrate.Flags().StringVarP(&dataDir, "data-dir", "d", "", "source directories for each node seperated by commas")
	cmdMigrate.Flags().StringVarP(&backupPath, "backup-dir", "b", "", "location of backup directory")
	cmdMigrate.Flags().BoolVar(&verbose, "verbose", false, "additional progress information")
	err := cmdMigrate.MarkFlagRequired("data-dir")
	if err != nil {
		glogger.Errorf("Error setting flag data-dir")
		return 1
	}
	err = cmdMigrate.MarkFlagRequired("backup-dir")
	if err != nil {
		glogger.Errorf("Error setting flag backup-dir")
		return 1
	}

	err = cmdMigrate.Execute()
	if err != nil {
		glogger.Errorf("exec error %v", err)
		return 1
	}
	return 0
}

func FetchFragments(base string) []string {
	var fragments []string

	ff := func(pathX string, infoX os.FileInfo, errX error) error {
		// first thing to do, check error. and decide what to do about it
		if errX != nil {
			glogger.Errorf("error 「%v」 at a path 「%q」\n", errX, pathX)
			return errX
		}
		pathX = pathX[len(base):]
		if !infoX.IsDir() {
			if strings.Contains(pathX, "fragment") && !strings.Contains(pathX, "cache") {
				fragments = append(fragments, pathX)
			}
		}
		return nil
	}

	err := filepath.Walk(base, ff)
	if err != nil {
		glogger.Errorf("error walking the path %q: %v\n", base, err)
	}
	return fragments
}

type local struct {
	Name      string              `json:"name,omitempty"`
	CreatedAt uint64              `json:"createdAt,omitempty"`
	Options   pilosa.IndexOptions `json:"options,omitempty"`
	Fields    []*pilosa.FieldInfo `json:"fields,omitempty"`
}

func fileExists(filename string) (bool, int64) {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false, 0
	}
	return !info.IsDir(), info.Size()
}

func BuildSchema(dataDir string) ([]byte, error) {
	// need to find all the ".meta" files and load as field options

	schemaSerializer := struct {
		Indexes []*local `json:"indexes,omitempty"`
	}{Indexes: make([]*local, 0)}
	var l *local
	ff := func(pathX string, infoX os.FileInfo, errX error) error {
		// first thing to do, check error. and decide what to do about it
		if errX != nil {
			glogger.Infof("error 「%v」 at a path 「%q」\n", errX, pathX)
			return errX
		}
		pathX = pathX[len(dataDir):]
		if infoX.IsDir() {
			// filepath.Walk(pathX, ff)
		} else {
			if strings.Contains(pathX, ".meta") {
				// convert the file to a fieldOptions
				// ex: metaPath /trait_store/aba/.meta
				glogger.Infof("PATHX %v", pathX)
				t := strings.Split(pathX, "/")
				index := t[1]
				src := dataDir + pathX
				content, err := os.ReadFile(src)
				if err != nil {
					return err
				}
				fstat, err := os.Stat(src)
				stat := fstat.Sys().(*syscall.Stat_t)
				if err != nil {
					return err
				}
				if len(t) == 3 {
					io, err := pilosa.UnmarshalIndexOptions(index, 0, content)
					if err != nil {
						return err
					}
					l = &local{
						Name:      index,
						Fields:    make([]*pilosa.FieldInfo, 0),
						CreatedAt: uint64(CTimeNano(stat)),
						Options:   *io,
					}
					// index options
					schemaSerializer.Indexes = append(schemaSerializer.Indexes, l)
					return nil
				}

				field := t[2]
				if field != "_exists" && field != "_keys" {
					fi, err := pilosa.UnmarshalFieldOptions(field, CTimeNano(stat), content)
					if err != nil {
						return err
					}
					l.Fields = append(l.Fields, fi)
				}
			}
		}
		return nil
	}

	err := filepath.Walk(dataDir, ff)
	if err != nil {
		glogger.Errorf("error walking the path %q: %v\n", dataDir, err)
	}
	return json.MarshalIndent(schemaSerializer, "", "    ")
}

func Extract(filename string) (index, field, view string, shard uint64) {
	// trait_store/aba/views/standard/fragments
	parts := strings.Split(filename, "/")
	shard, _ = strconv.ParseUint(parts[6], 10, 64)
	return parts[1], parts[2], parts[4], shard
}

// just a way to collect all the open dbs
type rbfFile struct {
	working *rbf.DB
	last    string
	temp    string
}

func (d *rbfFile) getDB(path, index string, shard uint64) (*rbf.DB, error) {
	src := fmt.Sprintf("%v/indexes/%v/shards/%04d", path, index, shard)
	if d.last != src {
		d.Close()
		d.last = src
		glogger.Infof("RBF: %v", src)
		c := cfg.NewDefaultConfig()
		c.FsyncEnabled = false
		c.MinWALCheckpointSize = 0
		db := rbf.NewDB(d.temp, c)
		err := db.Open()
		if err != nil {
			return nil, err
		}
		d.working = db
	}
	return d.working, nil
}

func (d *rbfFile) Close() error {
	defer func() error {
		// clean up the temp directory
		err := os.RemoveAll(d.temp)
		if err != nil {
			return err
		}
		return nil
	}()

	if d.last != "" {
		d.working.Close()

		// if d.last exists only keep the biggest
		exists, sz := fileExists(d.last)
		src := filepath.Join(d.temp, "data")
		if !exists {
			err := os.MkdirAll(filepath.Dir(d.last), 0o750)
			if err != nil {
				return err
			}
		} else {
			_, sz2 := fileExists(src)
			if sz > sz2 {
				return nil
			}
		}
		// move the datafile backup shard
		err := os.Rename(src, d.last)
		if err != nil {
			return err
		}
	}
	return nil
}

func copyFile(src, dest string) error {
	from, err := os.Open(src)
	if err != nil {
		return err
	}
	defer from.Close()

	to, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer to.Close()

	_, err = io.Copy(to, from)
	if err != nil {
		return err
	}
	return nil
}

func Migrate(dataDir, backupPath string, verbose bool) error {
	dataDir = strings.TrimSuffix(dataDir, "/")

	err := os.MkdirAll(backupPath, 0o750)
	if err != nil {
		return err
	}
	err = copyFile(filepath.Join(dataDir, "idalloc.db"), filepath.Join(backupPath, "idalloc"))
	if err != nil {
		return err
	}
	schema, err := BuildSchema(dataDir)
	if err != nil {
		return err
	}

	err = os.WriteFile(filepath.Join(backupPath, "schema"), schema, 0644)
	if err != nil {
		return err
	}

	raw := FetchFragments(dataDir)
	sort.Slice(raw, func(i, j int) bool {
		// trait_store/zip_code/views/standard/fragments/
		ti := strings.LastIndex(raw[i], "/") + 1
		shardi, err := strconv.ParseUint(raw[i][ti:], 10, 16)
		if err != nil {
			panic(err)
		}
		tj := strings.LastIndex(raw[j], "/") + 1
		shardj, err := strconv.ParseUint(raw[j][tj:], 10, 16)
		if err != nil {
			panic(err)
		}
		if shardi < shardj {
			return true
		} else if shardi == shardj {
			return strings.Compare(raw[i][:ti], raw[j][:tj]) < 0
		}
		return false
	})
	// raw is now sorted by shard

	cache := &rbfFile{
		temp: filepath.Join(backupPath, "_SCRATCH"),
	}
	bm := roaring.NewSliceBitmap()
	for _, filename := range raw {
		index, field, view, shard := Extract(filename)
		sz, before := visited[filename]
		fi, _ := os.Stat(dataDir + filename)
		if field != "_exists" {
			if !before {
				visited[filename] = fi.Size()
			} else {
				if fi.Size() <= sz {
					continue // skip it
				}
				visited[filename] = fi.Size()
			}
		}
		if verbose {
			glogger.Infof("processing: %v", dataDir+filename)
		}
		content, err := os.ReadFile(dataDir + filename)
		if err != nil {
			return err
		}
		err = bm.UnmarshalBinary(content)

		if err != nil {
			return err
		}
		db, err := cache.getDB(backupPath, index, shard)
		if err != nil {
			return err
		}
		key := string(txkey.Prefix(index, field, view, shard))
		tx, err := db.Begin(true)
		if err != nil {
			return err
		}
		tx.AddRoaring(key, bm)
		err = tx.Commit()
		if err != nil {
			return err
		}
	}
	cache.Close()
	keys := FetchIndexKeys(dataDir)
	for _, filename := range keys {
		glogger.Infof("index keys %v", filename)
		srcFile := filepath.Join(dataDir, filename)
		parts := strings.Split(filename, "/")
		destFile := filepath.Join(backupPath, "indexes", parts[1], "translate", parts[3])
		err = writeIfBigger(destFile, srcFile)
		if err != nil {
			return err
		}

	}
	// deal with index field(row)keys
	keys = FetchRowkeys(dataDir)
	for _, filename := range keys {
		glogger.Infof("field %v", filename)
		srcFile := dataDir + filename
		parts := strings.Split(filename, "/")
		destFile := filepath.Join(backupPath, "indexes", parts[1], "fields", parts[2], "translate")
		err = writeIfBigger(destFile, srcFile)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeIfBigger(dst string, srcFile string) error {
	if stats, err := os.Stat(dst); os.IsNotExist(err) {
		err = os.MkdirAll(filepath.Dir(dst), 0o750)
		if err != nil {
			return err
		}
		return copyFile(srcFile, dst)
	} else {
		stats2, err := os.Stat(srcFile)
		if err != nil {
			return err
		}
		if stats.Size() < stats2.Size() {
			vprint.VV("Bigger %v %v", stats.Size(), stats2.Size())
			return copyFile(srcFile, dst)
		}
	}
	return nil // simply skip it
}

func ignore(path string, items ...string) bool {
	f := filepath.Base(path)
	for i := range items {
		if items[i] == f {
			return true
		}
	}
	return false
}

func FetchIndexKeys(base string) []string {
	var directory []string

	ff := func(pathX string, infoX os.FileInfo, errX error) error {
		// first thing to do, check error. and decide what to do about it
		if errX != nil {
			glogger.Errorf("error 「%v」 at a path 「%q」\n", errX, pathX)
			return errX
		}
		pathX = pathX[len(base):]
		if infoX.IsDir() {
			// filepath.Walk(pathX, ff)
		} else {
			if strings.Contains(pathX, "_keys") && !ignore(pathX, ".data", "keys") {
				directory = append(directory, pathX)
			}
		}
		return nil
	}

	err := filepath.Walk(base, ff)
	if err != nil {
		glogger.Errorf("error walking the path %q: %v\n", base, err)
	}
	return directory
}

func FetchRowkeys(base string) []string {
	var directory []string

	ff := func(pathX string, infoX os.FileInfo, errX error) error {
		// first thing to do, check error. and decide what to do about it
		if errX != nil {
			glogger.Errorf("error 「%v」 at a path 「%q」\n", errX, pathX)
			return errX
		}
		pathX = pathX[len(base):]
		if infoX.IsDir() {
			// filepath.Walk(pathX, ff)
		} else {
			fp := filepath.Base(pathX)
			if fp == "keys" {
				p := strings.Split(pathX, "/")
				if len(p) != 4 {
					return nil // skip all but field/key files
				}
				directory = append(directory, pathX)
			}
		}
		return nil
	}

	err := filepath.Walk(base, ff)
	if err != nil {
		glogger.Errorf("error walking the path %q: %v\n", base, err)
	}
	return directory
}
