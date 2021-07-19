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

// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

/*
Package client enables querying a Pilosa server.

This client uses Pilosa's http+protobuf API.

Usage:

	import (
		"fmt"
		"github.com/molecula/featurebase/v2/client"
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
