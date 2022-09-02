// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

/*
Package client enables querying a Pilosa server.

This client uses Pilosa's http+protobuf API.

Usage:

	import (
		"fmt"
		"github.com/molecula/featurebase/v3/client"
	)

	// Create a Client instance
	cli := client.DefaultClient()

	// Create a Schema instance
	schema, err := cli.Schema()
	if err != nil {
		panic(err)
	}

	// Create an Index instance
	index, err := schema.Index("repository")
	if err != nil {
		panic(err)
	}

	// Create a Field instance
	stargazer, err := index.Field("stargazer")
	if err != nil {
		panic(err)
	}

	// Sync the schema with the server-side, so non-existing indexes/fields are created on the server-side.
	err = cli.SyncSchema(schema)
	if err != nil {
		panic(err)
	}

	// Execute a query
	response, err := cli.Query(stargazer.Row(5))
	if err != nil {
		panic(err)
	}

	// Act on the result
	fmt.Println(response.Result())

See also https://www.pilosa.com/docs/api-reference/ and https://www.pilosa.com/docs/query-language/.
*/
package client
