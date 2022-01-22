// Copyright 2021 Molecula Corp. All rights reserved.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/molecula/featurebase/v3/sql2"
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
