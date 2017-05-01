// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
