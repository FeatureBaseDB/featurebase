package cli

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	featurebase "github.com/featurebasedb/featurebase/v3"
	dax "github.com/featurebasedb/featurebase/v3/dax"
	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	t.Run("writeTable", func(t *testing.T) {
		wqr := &featurebase.WireQueryResponse{
			Schema: featurebase.WireQuerySchema{
				Fields: []*featurebase.WireQueryField{
					{Name: "_id", Type: dax.BaseTypeID},
					{Name: "name", Type: dax.BaseTypeString},
					{Name: "age", Type: dax.BaseTypeInt},
				},
			},
			Data: [][]interface{}{
				{1, "Amy", 44},
				{2, "Bob", 32},
				{3, "Cindy", 28},
			},
		}

		// TODO(tlt): used for debugging
		// format := defaultWriteOptions()
		// assert.NoError(t, writeTable(wqr, format, os.Stdout, os.Stdout, os.Stdout))
		// return

		tests := []struct {
			format  *writeOptions
			expQOut string
			expOut  string
			expErr  string
		}{
			{
				// default format
				format: defaultWriteOptions(),
				expQOut: stringOfLines(
					" _id | name  | age ",
					"-----+-------+-----",
					"   1 | Amy   |  44 ",
					"   2 | Bob   |  32 ",
					"   3 | Cindy |  28 ",
					"",
				),
				expOut: "",
				expErr: "",
			},
			{
				// timing on
				format: &writeOptions{
					border:     1,
					expanded:   false,
					timing:     true,
					tuplesOnly: false,
				},
				expQOut: stringOfLines(
					" _id | name  | age ",
					"-----+-------+-----",
					"   1 | Amy   |  44 ",
					"   2 | Bob   |  32 ",
					"   3 | Cindy |  28 ",
					"",
				),
				expOut: "Execution time: 0μs\n",
				expErr: "",
			},
			{
				// format.border = 2 (or higher)
				format: &writeOptions{
					border:     2,
					expanded:   false,
					timing:     false,
					tuplesOnly: false,
				},
				expQOut: stringOfLines(
					"+-----+-------+-----+",
					"| _id | name  | age |",
					"+-----+-------+-----+",
					"|   1 | Amy   |  44 |",
					"|   2 | Bob   |  32 |",
					"|   3 | Cindy |  28 |",
					"+-----+-------+-----+",
					"",
				),
				expOut: "",
				expErr: "",
			},
			{
				// format.border = 0
				format: &writeOptions{
					border:     0,
					expanded:   false,
					timing:     false,
					tuplesOnly: false,
				},
				expQOut: stringOfLines(
					"_id name  age",
					"--- ----- ---",
					"  1 Amy    44",
					"  2 Bob    32",
					"  3 Cindy  28",
					"",
				),
				expOut: "",
				expErr: "",
			},
			{
				// format.tuplesOnly = true
				format: &writeOptions{
					border:     1,
					expanded:   false,
					timing:     false,
					tuplesOnly: true,
				},
				expQOut: stringOfLines(
					" 1 | Amy   | 44 ",
					" 2 | Bob   | 32 ",
					" 3 | Cindy | 28 ",
					"",
				),
				expOut: "",
				expErr: "",
			},
			{
				// format.border = 2, expanded
				format: &writeOptions{
					border:     2,
					expanded:   true,
					timing:     false,
					tuplesOnly: false,
				},
				expQOut: stringOfLines(
					"+------+-------+",
					"| _id  | 1     |",
					"| name | Amy   |",
					"| age  | 44    |",
					"+------+-------+",
					"| _id  | 2     |",
					"| name | Bob   |",
					"| age  | 32    |",
					"+------+-------+",
					"| _id  | 3     |",
					"| name | Cindy |",
					"| age  | 28    |",
					"+------+-------+",
					"",
				),
				expOut: "",
				expErr: "",
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				// Set up buffers to capture the output.
				qOut := bytes.NewBuffer(make([]byte, 0, 100000))
				wOut := bytes.NewBuffer(make([]byte, 0, 100000))
				wErr := bytes.NewBuffer(make([]byte, 0, 100000))

				assert.NoError(t, writeOutput(wqr, test.format, qOut, wOut, wErr))

				assert.Equal(t, test.expQOut, qOut.String())
				assert.Equal(t, test.expOut, wOut.String())
				assert.Equal(t, test.expErr, wErr.String())
			})
		}
	})
}

func stringOfLines(lines ...string) string {
	var sb strings.Builder
	for _, line := range lines {
		sb.WriteString(line + "\n")
	}
	return sb.String()
}
