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
		t.Fatal(err)
	}

}
