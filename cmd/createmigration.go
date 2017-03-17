package cmd

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/pilosa/pilosa/ctl"
)

var migrationPlanner = ctl.NewCreateMigrationCommand(os.Stdin, os.Stdout, os.Stderr)

var createMigrationPlanCmd = &cobra.Command{
	Use:   "create-migration",
	Short: "Create a migration plan file for cluster topology changes",
	Long: `This command produces a file used as input for the run-migration command .
`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := migrationPlanner.Run(context.Background()); err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	createMigrationPlanCmd.Flags().StringVarP(&migrationPlanner.Host, "host", "", "localhost:15000", "host:port of Pilosa providing the schema.")
	createMigrationPlanCmd.Flags().StringVarP(&migrationPlanner.SrcConfig, "srcConfig", "", "", "Original Cluster Node Configuration File")
	createMigrationPlanCmd.Flags().StringVarP(&migrationPlanner.DestConfig, "destConfig", "", "", "Proposed Cluster Node Configuration File")

	err := viper.BindPFlags(createMigrationPlanCmd.Flags())
	if err != nil {
		log.Fatalf("Error binding backup flags: %v", err)
	}

	RootCmd.AddCommand(createMigrationPlanCmd)
}
