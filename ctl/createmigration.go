package ctl

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
)

// BackupCommand represents a command for backing up a frame.
type CreateMigrationCommand struct {
	// host/port for pilosa server providing the schema needing to be migrated
	Host string

	// path to the configuration files
	SrcConfig  string
	DestConfig string

	OutputFileName string

	// Standard input/output
	*pilosa.CmdIO
}

// NewBackupCommand returns a new instance of BackupCommand.
func NewCreateMigrationCommand(stdin io.Reader, stdout, stderr io.Writer) *CreateMigrationCommand {
	return &CreateMigrationCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

func (cmd *CreateMigrationCommand) Run(ctx context.Context) error {

	client, err := pilosa.NewClient(cmd.Host)
	if err != nil {
		log.Fatal(err)
	}

	schema, err := client.Schema(ctx)
	if err != nil {
		log.Fatal(err)
	}

	srcConfig := pilosa.NewConfig()
	destConfig := pilosa.NewConfig()

	if _, err := toml.DecodeFile(cmd.SrcConfig, srcConfig); err != nil {
		log.Fatal(err)
	}
	if _, err := toml.DecodeFile(cmd.DestConfig, destConfig); err != nil {
		log.Fatal(err)
	}

	srcCluster := srcConfig.PilosaCluster()
	destCluster := destConfig.PilosaCluster()

	plan := NewMigrationPlan(srcConfig.DataDir, destConfig.DataDir)

	BuildPlan(schema, srcCluster, destCluster, plan)

	//either write it to a file or stdout
	if cmd.OutputFileName == "-" {
		json.NewEncoder(os.Stdout).Encode(plan)
	} else {
		f, err := os.Create(cmd.OutputFileName)
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

func BuildPlan(schema []*internal.DB, srcCluster, destCluster *pilosa.Cluster, plan *Plan) {

	srcLookup := make(map[string]struct{})
	destLookup := make(map[string]struct{})

	for _, dbinfo := range schema {
		for slice := uint64(0); slice <= dbinfo.MaxSlice; slice++ {
			for _, src := range srcCluster.FragmentNodes(dbinfo.Name, slice) {
				key := make_key(dbinfo.Name, slice, clean(src.Host))
				srcLookup[key] = struct{}{}
			}
		}
	}
	for _, dbinfo := range schema {
		for slice := uint64(0); slice <= dbinfo.MaxSlice; slice++ {
			for _, src := range srcCluster.FragmentNodes(dbinfo.Name, slice) {
				for _, dest := range destCluster.FragmentNodes(dbinfo.Name, slice) {
					key := make_key(dbinfo.Name, slice, clean(dest.Host))
					destLookup[key] = struct{}{}
					_, found := srcLookup[key]
					if !found {
						for _, frame := range dbinfo.Frames {
							plan.Actions[src.Host] = append(plan.Actions[src.Host],
								&Destination{
									Db:    dbinfo.Name,
									Frame: frame.Name,
									Slice: slice,
									Host:  clean(dest.Host),
								})
						}
					}
				}

			}
		}
	}
}
