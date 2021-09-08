// Copyright 2021 Pilosa Corp.
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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/molecula/featurebase/v2/sql2"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err == flag.ErrHelp {
		os.Exit(1)
	} else if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("featurebase-parse-sql", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	q := fs.Arg(0)
	if q == "" {
		return fmt.Errorf("query required")
	}

	stmt, err := sql2.NewParser(strings.NewReader(q)).ParseStatement()
	if err != nil {
		return err
	}

	buf, err := json.MarshalIndent(stmt, "", "    ")
	if err != nil {
		return err
	}
	fmt.Println(string(buf))

	return nil
}
