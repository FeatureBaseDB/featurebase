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

package pilosa_test

import (
	"context"
	"testing"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/test"
)

func TestPlanner_Count(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	index, err := c.GetHolder(0).CreateIndex("i", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	if _, err := index.CreateField("f"); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: "i",
		Query: `
			Set(1, f=10)
			Set(2, f=10)
			Set(3, f=11)
	`}); err != nil {
		t.Fatal(err)
	}

	// Parse SQL into AST.
	q := `SELECT COUNT(*) AS "count" FROM i`
	stmt, err := c.GetNode(0).Server.PlanSQL(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// Scan first row from result set.
	var n int
	if err := stmt.QueryRowContext(context.Background()).Scan(&n); err != nil {
		t.Fatal(err)
	} else if got, want := n, 3; got != want {
		t.Fatalf("Scan()=%d, want %d", got, want)
	}

	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
}
