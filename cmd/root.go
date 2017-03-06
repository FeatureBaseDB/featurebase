package cmd

import "github.com/spf13/cobra"

var (
	Version   string
	BuildTime string
)

var RootCmd = &cobra.Command{
	Use:   "pilosa",
	Short: "Pilosa - A Distributed In-memory Binary Bitmap Index.",
	// TODO - is documentation actually there?
	Long: `Pilosa is a fast index to turbocharge your database.

This binary contains Pilosa itself, as well as common
tools for administering pilosa, importing/exporting data,
backing up, and more. Complete documentation is available
at http://pilosa.com/docs

`,
}

func init() {
	if Version == "" {
		Version = "v0.0.0"
	}
	if BuildTime == "" {
		BuildTime = "not recorded"
	}

	RootCmd.Long = RootCmd.Long + "Version: " + Version + "\nBuild Time: " + BuildTime + "\n"
}
