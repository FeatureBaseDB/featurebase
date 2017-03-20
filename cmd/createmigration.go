package cmd

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var MigrationPlanner *ctl.CreateMigrationCommand

func NewMigrationCmd(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	MigrationPlanner = ctl.NewCreateMigrationCommand(os.Stdin, os.Stdout, os.Stderr)
	migrationCmd := &cobra.Command{
		Use:   "create-migration",
		Short: "Create a migration plan file for cluster topology changes",
		Long: `This command produces a file used as input for the run-migration command .
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return MigrationPlanner.Run(context.Background())
		},
	}
	flags := migrationCmd.Flags()
	flags.StringVarP(&MigrationPlanner.Host, "host", "p", "localhost:15000", "host:port of Pilosa providing the schema.")
	flags.StringVarP(&MigrationPlanner.SrcConfig, "src-config", "s", "", "Original Cluster Node Configuration File")
	flags.StringVarP(&MigrationPlanner.DestConfig, "dest-config", "d", "", "Proposed Cluster Node Configuration File")
	flags.StringVarP(&MigrationPlanner.OutputFileName, "output-file", "o", "-", "filename to store the plan")
	return migrationCmd
}

func init() {
	subcommandFns["create-migration"] = NewMigrationCmd
}
