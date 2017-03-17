package ctl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pilosa/pilosa"
)

// BackupCommand represents a command for backing up a frame.
type CreateMigrationCommand struct {
	// host/port for pilosa server providing the schema needing to be migrated
	Host string

	// path to the configuration files
	SrcConfig  string
	DestConfig string

	DestFileName string

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewBackupCommand returns a new instance of BackupCommand.
func NewCreateMigrationCommand(stdin io.Reader, stdout, stderr io.Writer) *CreateMigrationCommand {
	return &CreateMigrationCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr}
}

func (cmd *CreateMigrationCommand) Run(ctx context.Context) error {

	plan := BuildPlan(cmd.Host, cmd.SrcConfig, cmd.DestConfig)
	//either write it to a file or stdout
	if cmd.DestFileName == "-" {
		json.NewEncoder(os.Stdout).Encode(plan)
	} else {
		f, err := os.Open(cmd.DestFileName)
		if err != nil {
			log.Fatal("Error:", err)
		}
		json.NewEncoder(f).Encode(plan)

	}

	return nil
}

type Plan struct {
	Actions     map[string][]*Destination
	SrcDataDir  string
	DestDataDir string
}

func NewMigrationPlan(srcpath, destpath string) *Plan {
	return &Plan{
		Actions:     make(map[string][]*Destination),
		SrcDataDir:  srcpath,
		DestDataDir: destpath}
}

type MaxSliceResponse struct {
	MaxSlices map[string]uint64 `json:"MaxSlices"`
}

type frame struct {
	Name string `json:"name"`
}

type db struct {
	Frames []frame `json:"frames"`
	Name   string  `json:"name"`
}

type SchemaResponse struct {
	Dbs []db `json:"dbs"`
}

func getFrames(s SchemaResponse, dbv string) []string {
	var ret []string
	for _, db := range s.Dbs {
		if db.Name == dbv {
			for _, v := range db.Frames {
				ret = append(ret, v.Name)
			}
			return ret
		}
	}
	return ret
}

func make_key(db string, slice uint64, host string) string {
	var buffer bytes.Buffer
	buffer.WriteString(db)
	buffer.WriteString("|")
	buffer.WriteString(strconv.FormatUint(slice, 10))
	buffer.WriteString("|")
	buffer.WriteString(host)
	return buffer.String()
}

type Destination struct {
	Db    string `json:"db"`
	Frame string `json:"frame"`
	Slice uint64 `json:"slice"`
	Host  string `json:"host"`
}

func clean(host string) string {
	parts := strings.Split(host, ":")
	if len(parts) > 1 {
		return parts[0]
	}
	return host
}

func dumpNarrative(d map[string][]*Destination, srcConfig, destConfig *pilosa.Config) {
	for srchost, v := range d {
		fmt.Println("# GOTO host:", srchost)
		remfiles := make(map[string]struct{})
		for _, part := range v {
			destBase := destConfig.DataDir + "/" + part.Db + "/" + part.Frame
			fmt.Printf("#make remote directory if doesn't exist %s:%s\n", part.Host, destBase)
			srcBase := srcConfig.DataDir + "/" + part.Db + "/" + part.Frame + "/" + strconv.FormatUint(part.Slice, 10)
			destBase = destBase + "/" + strconv.FormatUint(part.Slice, 10)
			fmt.Printf("# copy local file %s to %s:%s\n", srcBase, part.Host, destBase)
			remfiles[srcBase] = struct{}{}
			//fmt.Printf("# remove local file %s \n", srcBase)
			srcBase += ".cache"
			destBase += ".cache"
			fmt.Printf("# copy local file %s to %s:%s\n", srcBase, part.Host, destBase)
			remfiles[srcBase] = struct{}{}
		}
		for f, _ := range remfiles {
			fmt.Printf("# remove local file %s \n", f)
		}
		fmt.Println("# DONE host:", srchost)
		fmt.Println("")

	}

}

func dumpClusterState(state map[string]struct{}, out io.Writer) {
	results := make([]struct {
		Db    string `json:db`
		Slice int    `json:slice`
		Host  string `json:host`
	}, 0, len(state))
	for k, _ := range state {
		p := strings.Split(k, "|")
		v, _ := strconv.Atoi(p[1])
		results = append(results, struct {
			Db    string `json:db`
			Slice int    `json:slice`
			Host  string `json:host`
		}{p[0], v, p[2]})
	}
	json.NewEncoder(out).Encode(results)
}

func diff(X, Y []string) []string {
	difference := make([]string, 0)
	var i, j int
	for i < len(X) && j < len(Y) {
		if X[i] < Y[j] {
			difference = append(difference, X[i])
			i++
		} else if X[i] > Y[j] {
			j++
		} else { //X[i] == Y[j]
			j++
			i++
		}
	}
	if i < len(X) { //All remaining in X are greater than Y, just copy over
		finalLength := len(X) - i + len(difference)
		if finalLength > cap(difference) {
			newDifference := make([]string, finalLength)
			copy(newDifference, difference)
			copy(newDifference[len(difference):], X[i:])
			difference = newDifference
		} else {
			differenceLen := len(difference)
			difference = difference[:finalLength]
			copy(difference[differenceLen:], X[i:])
		}
	}
	return difference
}

func sortKeys(m map[string]struct{}) []string {
	keys := make([]string, len(m), len(m))
	for k, _ := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func BuildPlan(server, srcConfigurationPath, destConfigurationPath string) *Plan {

	var m MaxSliceResponse
	var s SchemaResponse

	// Fetch /slices/max
	response, err := http.Get("http://" + server + "/slices/max")
	if err != nil {
		log.Fatal(err)
	} else {
		defer response.Body.Close()
		responseData, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatal(err)
		}
		if err := json.Unmarshal(responseData, &m); err != nil {
			log.Fatal(err)
		}
	}
	// fetch /schema
	response, err = http.Get("http://" + server + "/schema")
	if err != nil {
		log.Fatal(err)
	} else {
		defer response.Body.Close()
		responseData, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatal(err)
		}
		if err := json.Unmarshal(responseData, &s); err != nil {
			log.Fatal(err)
		}
	}
	srcConfig := pilosa.NewConfig()
	destConfig := pilosa.NewConfig()

	if _, err := toml.DecodeFile(srcConfigurationPath, srcConfig); err != nil {
		log.Fatal(err)
	}
	if _, err := toml.DecodeFile(destConfigurationPath, destConfig); err != nil {
		log.Fatal(err)
	}

	srcCluster := srcConfig.PilosaCluster()
	destCluster := destConfig.PilosaCluster()

	srcLookup := make(map[string]struct{})
	destLookup := make(map[string]struct{})

	for db, maxs := range m.MaxSlices {
		for slice := uint64(0); slice <= maxs; slice++ {
			for _, src := range srcCluster.FragmentNodes(db, slice) {
				key := make_key(db, slice, clean(src.Host))
				srcLookup[key] = struct{}{}
			}
		}
	}
	plan := NewMigrationPlan(srcConfig.DataDir, destConfig.DataDir)
	for db, maxs := range m.MaxSlices {
		frames := getFrames(s, db)
		for slice := uint64(0); slice <= maxs; slice++ {
			for _, src := range srcCluster.FragmentNodes(db, slice) {
				for _, dest := range destCluster.FragmentNodes(db, slice) {
					key := make_key(db, slice, clean(dest.Host))
					destLookup[key] = struct{}{}
					_, found := srcLookup[key]
					if !found {
						for _, frame := range frames {
							plan.Actions[src.Host] = append(plan.Actions[src.Host],
								&Destination{
									Db:    db,
									Frame: frame,
									Slice: slice,
									Host:  clean(dest.Host),
								})
						}
					}
				}

			}
		}
	}
	return plan
}
