// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
/*
Package cmd contains all the pilosa subcommand definitions (1 per file).

Each command file has an init function and a New*Cmd function, as well as a
global exported instance of the command.

The New*Cmd function is a function which returns a cobra.Command object wrapping this subcommand.

The init function adds the New*Cmd to a map of subcommand functions which
ensures that no two commands have the same name, and is used when a new root
command is created to instantiate all of the subcommands.

The instance of the command is global and exported so that it can be tested.
*/
package cmd
