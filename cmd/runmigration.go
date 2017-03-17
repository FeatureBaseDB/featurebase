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

var migrationRunner = ctl.NewRunMigrationCommand(os.Stdin, os.Stdout, os.Stderr)

var runMigrationPlanCmd = &cobra.Command{
	Use:   "run-migration",
	Short: "Execute a migration plan file for cluster topology changes",
	Long: `This command migrates the data to a new cluster configuration .
`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := migrationRunner.Run(context.Background()); err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	runMigrationPlanCmd.Flags().StringVarP(&migrationRunner.Plan, "plan", "", "", "migration plane json file")
	runMigrationPlanCmd.Flags().StringVarP(&migrationRunner.User, "user", "", "", "ssh username for deployment")
	runMigrationPlanCmd.Flags().StringVarP(&migrationRunner.PemFile, "pem", "", "", "private key filename assiated with the ssh user")

	err := viper.BindPFlags(runMigrationPlanCmd.Flags())
	if err != nil {
		log.Fatalf("Error binding run-migration flags: %v", err)
	}

	RootCmd.AddCommand(runMigrationPlanCmd)
}
