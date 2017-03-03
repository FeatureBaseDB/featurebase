package cmd

import "github.com/spf13/cobra"

var RootCmd = &cobra.Command{
	Use:   "pilosa",
	Short: "pilosa - A Distributed In-memory Binary Bitmap Index",
	Long: `Pilosa is a fast index to turbocharge your database.

This binary contains Pilosa itself, as well as common
tools for administering pilosa, importing/exporting data,
backing up, and more. Complete documentation is available
at http://pilosa.com/docs`, // TODO - is documentation actually there?
}
