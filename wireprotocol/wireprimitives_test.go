package wireprotocol_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/wireprotocol"
	"github.com/google/go-cmp/cmp"
)

func TestWireProtocol_Schema(t *testing.T) {

	s := types.Schema{
		&types.PlannerColumn{
			ColumnName: "col1",
			Type:       parser.NewDataTypeID(),
		},
		&types.PlannerColumn{
			ColumnName: "col2",
			Type:       parser.NewDataTypeInt(),
		},
		&types.PlannerColumn{
			ColumnName: "col3",
			Type:       parser.NewDataTypeDecimal(4),
		},
		&types.PlannerColumn{
			ColumnName: "col4",
			Type:       parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			ColumnName: "col5",
			Type:       parser.NewDataTypeStringSet(),
		},
		&types.PlannerColumn{
			ColumnName: "col6",
			Type:       parser.NewDataTypeIDSet(),
		},
		&types.PlannerColumn{
			ColumnName: "col7",
			Type:       parser.NewDataTypeBool(),
		},
		&types.PlannerColumn{
			ColumnName: "col8",
			Type:       parser.NewDataTypeTimestamp(),
		},
	}

	b, err := wireprotocol.WriteSchema(s)
	if err != nil {
		t.Fatal(err)
	}

	rdr := bytes.NewReader(b)
	_, err = wireprotocol.ExpectToken(rdr, wireprotocol.TOKEN_SCHEMA_INFO)
	if err != nil {
		t.Fatal(err)
	}

	sr, err := wireprotocol.ReadSchema(rdr)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(s, sr); diff != "" {
		t.Fatal(diff)
	}
}

func TestWireProtocol_Row(t *testing.T) {

	s := types.Schema{
		&types.PlannerColumn{
			ColumnName: "col1",
			Type:       parser.NewDataTypeID(),
		},
		&types.PlannerColumn{
			ColumnName: "col2",
			Type:       parser.NewDataTypeInt(),
		},
		&types.PlannerColumn{
			ColumnName: "col3",
			Type:       parser.NewDataTypeDecimal(4),
		},
		&types.PlannerColumn{
			ColumnName: "col4",
			Type:       parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			ColumnName: "col5",
			Type:       parser.NewDataTypeStringSet(),
		},
		&types.PlannerColumn{
			ColumnName: "col6",
			Type:       parser.NewDataTypeIDSet(),
		},
		&types.PlannerColumn{
			ColumnName: "col7",
			Type:       parser.NewDataTypeBool(),
		},
		&types.PlannerColumn{
			ColumnName: "col8",
			Type:       parser.NewDataTypeTimestamp(),
		},
	}

	r := types.Row{
		int64(1),
		int64(2),
		pql.NewDecimal(123400, 4),
		string("foo"),
		[]string{"bar", "baz"},
		[]int64{10, 20},
		bool(false),
		time.Now().UTC(),
	}

	b, err := wireprotocol.WriteRow(r, s)
	if err != nil {
		t.Fatal(err)
	}

	rdr := bytes.NewReader(b)
	_, err = wireprotocol.ExpectToken(rdr, wireprotocol.TOKEN_ROW)
	if err != nil {
		t.Fatal(err)
	}

	rr, err := wireprotocol.ReadRow(rdr, s)
	if err != nil {
		t.Fatal(err)
	}

	opt := cmp.Comparer(func(x, y pql.Decimal) bool {
		return x.EqualTo(y)
	})

	if diff := cmp.Diff(r, rr, opt); diff != "" {
		t.Fatal(diff)
	}
}
