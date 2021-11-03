// Copyright 2020 Pilosa Corp.
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
package sql_test

import (
	"context"
	"testing"

	"github.com/molecula/featurebase/v2/sql"
	"github.com/molecula/featurebase/v2/test"
)

func TestHandler(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	api := cluster.GetNode(0).API
	queryStr := "select * from nowhere"
	mapper := sql.NewMapper()
	query, err := mapper.MapSQL(queryStr)
	if err != nil {
		t.Fatal("failed to map SQL")
	}
	handler := sql.NewSelectHandler(api)
	_, err = handler.Handle(context.Background(), query)
	if err.Error() != "mapping select: handling: nowhere: index not found" {
		//expecting it to fail with index not found
		//can be more elaborate later
		t.Fatal(err)
	}

}
