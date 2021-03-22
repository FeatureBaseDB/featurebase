// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

/*
Package pilosa enables querying a Pilosa server.

This client uses Pilosa's http+protobuf API.

Usage:

	import (
		"fmt"
		"github.com/pilosa/pilosa/v2/client"
	)

	// Create a Client instance
	client := client.DefaultClient()

	// Create a Schema instance
	schema, err := client.Schema()
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
	err = client.SyncSchema(schema)
	if err != nil {
		panic(err)
	}

	// Execute a query
	response, err := client.Query(stargazer.Row(5))
	if err != nil {
		panic(err)
	}

	// Act on the result
	fmt.Println(response.Result())

See also https://www.pilosa.com/docs/api-reference/ and https://www.pilosa.com/docs/query-language/.
*/
package client
