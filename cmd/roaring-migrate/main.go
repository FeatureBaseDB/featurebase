package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/rbf"
	"github.com/pilosa/pilosa/v2/rbf/cfg"
	"github.com/pilosa/pilosa/v2/roaring"
	txkey "github.com/pilosa/pilosa/v2/short_txkey"
	"github.com/spf13/cobra"
)

func main() {
	var dataDir, backupPath string
	cmdMigrate := &cobra.Command{
		Use:   "roaring-migrate",
		Short: "TBD",
		Long:  ` TBD`,
		//Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			nodes := strings.Split(dataDir, ",")
			for _, nodePath := range nodes {
				err := Migrate(nodePath, backupPath)
				if err != nil {
					fmt.Println("Error", err)
					return
				}

			}
		},
	}
	cmdMigrate.Flags().StringVarP(&dataDir, "dataDir", "d", "", "source directories for each node seperated by commas")
	cmdMigrate.Flags().StringVarP(&backupPath, "backupDir", "b", "", "location of backup directory")
	cmdMigrate.MarkFlagRequired("dataDir")
	cmdMigrate.MarkFlagRequired("backupDir")

	err := cmdMigrate.Execute()
	if err != nil {
		fmt.Println("exec error", err)
	}
}

func FetchFragments(base string) []string {
	var directory []string

	ff := func(pathX string, infoX os.FileInfo, errX error) error {

		// first thing to do, check error. and decide what to do about it
		if errX != nil {
			fmt.Printf("error 「%v」 at a path 「%q」\n", errX, pathX)
			return errX
		}
		pathX = pathX[len(base):]
		if !infoX.IsDir() {
			if strings.Contains(pathX, "fragment") && !strings.Contains(pathX, "cache") {
				directory = append(directory, pathX)
			}
		}
		return nil
	}

	err := filepath.Walk(base, ff)

	if err != nil {
		fmt.Printf("error walking the path %q: %v\n", base, err)
	}
	return directory
}

type local struct {
	Name      string              `json:"name,omitempty"`
	CreatedAt uint64              `json:"createdAt,omitempty"`
	Options   pilosa.IndexOptions `json:"options,omitempty"`
	Fields    []*pilosa.FieldInfo `json:"fields,omitempty"`
}

func BuildSchema(dataDir string) ([]byte, error) {
	//need to find all the ".meta" files and load as field options

	schemaSerializer := struct {
		Indexes []*local `json:"indexes,omitempty"`
	}{Indexes: make([]*local, 0)}
	var l *local
	ff := func(pathX string, infoX os.FileInfo, errX error) error {

		// first thing to do, check error. and decide what to do about it
		if errX != nil {
			fmt.Printf("error 「%v」 at a path 「%q」\n", errX, pathX)
			return errX
		}
		pathX = pathX[len(dataDir):]
		if infoX.IsDir() {
			//filepath.Walk(pathX, ff)
		} else {
			if strings.Contains(pathX, ".meta") {
				//convert the file to a fieldOptions
				// ex: metaPath /trait_store/aba/.meta
				fmt.Println("PATHX", pathX)
				t := strings.Split(pathX, "/")
				index := t[1]
				src := dataDir + pathX
				content, err := ioutil.ReadFile(src)
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
						CreatedAt: uint64(stat.Ctim.Nano()),
						Options:   *io,
					}
					//index options
					schemaSerializer.Indexes = append(schemaSerializer.Indexes, l)
					return nil
				}

				field := t[2]
				if field != "_exists" {
					fi, err := pilosa.UnmarshalFieldOptions(field, stat.Ctim.Nano(), content)
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
		fmt.Printf("error walking the path %q: %v\n", dataDir, err)
	}
	return json.MarshalIndent(schemaSerializer, "", "    ")
}
func Extract(filename string) (index, field, view string, shard uint64) {
	//trait_store/aba/views/standard/fragments
	parts := strings.Split(filename, "/")
	shard, _ = strconv.ParseUint(parts[6], 10, 64)
	return parts[1], parts[2], parts[4], shard
}

//just a way to collect all the open dbs
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
		fmt.Println("RBF:", src)
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
	if d.last != "" {
		d.working.Close()
		//if d.last exists only keep the biggest
		os.MkdirAll(filepath.Dir(d.last), 0777)
		// move the datafile backup shard
		os.Rename(d.temp+"/data", d.last)
		//cleanup the tempdirectory
		os.RemoveAll(d.temp)
	}
	return nil
}
func copyFile(src, dest string) error {
	input, err := ioutil.ReadFile(src)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(dest, input, 0644)
	if err != nil {
		return err
	}
	return nil
}

func Migrate(dataDir, backupPath string) error {
	os.MkdirAll(backupPath, 0777)
	err := copyFile(dataDir+"/idalloc.db", backupPath+"idalloc")
	if err != nil {
		return err
	}
	schema, err := BuildSchema(dataDir)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(backupPath+"/schema", schema, 0644)
	if err != nil {
		return err
	}

	raw := FetchFragments(dataDir)
	sort.Slice(raw, func(i, j int) bool {
		//trait_store/zip_code/views/standard/fragments/
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
	//raw is now sorted by shard

	// need index/field/shard
	// make rbf file in backup
	rowSize := uint64(0) //?
	clear := false
	log := false
	cache := &rbfFile{
		temp: backupPath + "/_SCRATCH",
	}
	for _, filename := range raw {
		index, field, view, shard := Extract(filename)
		content, err := ioutil.ReadFile(dataDir + filename)
		if err != nil {
			return err
		}
		itr, err := roaring.NewRoaringIterator(content)
		if err != nil {
			return err
		}
		db, err := cache.getDB(backupPath, index, shard)
		if err != nil {
			return err
		}
		tx, err := db.Begin(true)
		if err != nil {
			return err
		}
		key := string(txkey.Prefix(index, field, view, shard))
		_, _, err = tx.ImportRoaringBits(key, itr, clear, log, rowSize, nil)
		if err != nil {
			tx.Rollback()
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
	}
	cache.Close()
	keys := FetchIndexKeys(dataDir)
	for _, filename := range keys {
		fmt.Println("index keys", filename)
		content, err := ioutil.ReadFile(dataDir + filename)
		if err != nil {
			return err
		}
		parts := strings.Split(filename, "/")
		destFile := fmt.Sprintf("%v/indexes/%v/translate/%v", backupPath, parts[1], parts[3])
		err = writeIfBigger(destFile, content)
		if err != nil {
			return err
		}

	}
	//deal with index field(row)keys
	keys = FetchRowkeys(dataDir)
	for _, filename := range keys {
		fmt.Println("field", filename)
		content, err := ioutil.ReadFile(dataDir + filename)
		if err != nil {
			return err
		}
		parts := strings.Split(filename, "/")
		destFile := fmt.Sprintf("%v/indexes/%v/fields/%v/translate", backupPath, parts[1], parts[2])
		err = writeIfBigger(destFile, content)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeIfBigger(dst string, content []byte) error {
	if stats, err := os.Stat(dst); os.IsNotExist(err) {
		os.MkdirAll(filepath.Dir(dst), 0777)
		return ioutil.WriteFile(dst, content, 0644)
	} else {
		if stats.Size() < int64(len(content)) {
			return ioutil.WriteFile(dst, content, 0644)
		}
	}
	return nil //simply skipp it
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

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func FetchIndexKeys(base string) []string {
	var directory []string

	ff := func(pathX string, infoX os.FileInfo, errX error) error {

		// first thing to do, check error. and decide what to do about it
		if errX != nil {
			fmt.Printf("error 「%v」 at a path 「%q」\n", errX, pathX)
			return errX
		}
		pathX = pathX[len(base):]
		if infoX.IsDir() {
			//filepath.Walk(pathX, ff)
		} else {
			if strings.Contains(pathX, "_keys") && !ignore(pathX, ".data", "keys") {
				directory = append(directory, pathX)
			}
		}
		return nil
	}

	err := filepath.Walk(base, ff)

	if err != nil {
		fmt.Printf("error walking the path %q: %v\n", base, err)
	}
	return directory
}

func FetchRowkeys(base string) []string {
	var directory []string

	ff := func(pathX string, infoX os.FileInfo, errX error) error {

		// first thing to do, check error. and decide what to do about it
		if errX != nil {
			fmt.Printf("error 「%v」 at a path 「%q」\n", errX, pathX)
			return errX
		}
		pathX = pathX[len(base):]
		if infoX.IsDir() {
			//filepath.Walk(pathX, ff)
		} else {
			fp := filepath.Base(pathX)
			if fp == "keys" {
				p := strings.Split(pathX, "/")
				if len(p) != 4 {
					return nil //skip all but field/key files
				}
				directory = append(directory, pathX)
			}
		}
		return nil
	}

	err := filepath.Walk(base, ff)

	if err != nil {
		fmt.Printf("error walking the path %q: %v\n", base, err)
	}
	return directory
}
