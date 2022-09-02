// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
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

	stmt, err := parser.NewParser(strings.NewReader(q)).ParseStatement()
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
