package batch

import (
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/stretchr/testify/assert"
)

func TestBatchSQL(t *testing.T) {
	tbl := &dax.Table{
		Name: "foo",
	}
	fields := []*dax.Field{
		{
			Name: "name",
			Type: dax.BaseTypeString,
		},
		{
			Name: "age",
			Type: dax.BaseTypeInt,
		},
	}
	ids := []interface{}{
		0, 1, 2,
	}
	rows := [][]interface{}{
		{
			[]interface{}{"Alice", int64(11)},
		},
		{
			[]interface{}{"Bob", int64(22)},
		},
		{
			[]interface{}{"Carl,Comma", int64(33)},
		},
	}

	s, err := buildBulkInsert(tbl, fields, ids, rows)
	assert.NoError(t, err)

	exp := `BULK INSERT INTO foo (_id,name,age) MAP (0 int,1 string,2 int) FROM x'0,Alice,11
1,Bob,22
2,"Carl,Comma",33
' WITH BATCHSIZE 3 FORMAT 'CSV' INPUT 'STREAM'`
	assert.Equal(t, exp, s)
}
