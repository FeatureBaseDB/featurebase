package batch

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	fbbatch "github.com/featurebasedb/featurebase/v3/batch"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/pql"
)

// Ensure type implements interface.
var _ fbbatch.Batcher = (*sqlBatcher)(nil)

type sqlBatcher struct {
	inserter Inserter
	fields   []*dax.Field
}

func NewSQLBatcher(i Inserter, flds []*dax.Field) *sqlBatcher {
	return &sqlBatcher{
		inserter: i,
		fields:   flds,
	}
}

func (b *sqlBatcher) NewBatch(cfg fbbatch.Config, tbl *dax.Table, flds []*dax.Field) (fbbatch.RecordBatch, error) {
	fields := flds
	if b.fields != nil {
		fields = b.fields
	}
	return &sqlBatch{
		table:        tbl,
		fields:       fields,
		size:         cfg.Size,
		maxStaleness: cfg.MaxStaleness,
		ids:          make([]interface{}, 0, cfg.Size),
		rows:         make([][]interface{}, 0, cfg.Size),
		inserter:     b.inserter,
	}, nil
}

// Ensure type implements interface.
var _ fbbatch.RecordBatch = (*sqlBatch)(nil)

type sqlBatch struct {
	table  *dax.Table
	fields []*dax.Field
	size   int

	ids  []interface{}
	rows [][]interface{}

	// staleTime tracks the time the first record of the batch was inserted
	// plus the maxStaleness, in order to raise ErrBatchNowStale if the
	// maxStaleness has elapsed
	staleTime    time.Time
	maxStaleness time.Duration

	// inserter handles SQL INSERT statements generated for each batch.
	inserter Inserter
}

func (b *sqlBatch) Add(rec fbbatch.Row) error {
	// Clear rec.Values and rec.Clears upon return.
	defer func() {
		for i := range rec.Values {
			rec.Values[i] = nil
		}
		for k := range rec.Clears {
			delete(rec.Clears, k)
		}
	}()

	if len(b.ids) == cap(b.ids) {
		return fbbatch.ErrBatchAlreadyFull
	}
	if len(rec.Values) != len(b.fields) {
		return errors.Errorf("record needs to match up with batch fields, got %d fields and %d record", len(b.fields), len(rec.Values))
	}

	// Append the ID to b.ids.
	b.ids = append(b.ids, rec.ID)

	// Convert decimal fields (which come in as int64, along with the scale in
	// field) to pql.Decimal.
	for i, fld := range b.fields {
		switch b.fields[i].Type {
		case dax.BaseTypeDecimal:
			if val, ok := rec.Values[i].(int64); ok {
				rec.Values[i] = pql.NewDecimal(val, fld.Options.Scale)
			}
		case dax.BaseTypeTimestamp:
			if val, ok := rec.Values[i].(int64); ok {
				ts := time.Unix(val, 0)
				rec.Values[i] = ts.Format(time.RFC3339)
			}
		}
	}

	// Append the record to b.rows.
	vals := make([]interface{}, 0, len(rec.Values))
	vals = append(vals, rec.Values...)
	b.rows = append(b.rows, vals)

	// Check for batch full or stale.
	if len(b.ids) == cap(b.ids) {
		return fbbatch.ErrBatchNowFull
	}
	if b.maxStaleness != time.Duration(0) { // set maxStaleness to 0 to disable staleness checking
		if len(b.ids) == 1 {
			b.staleTime = time.Now().Add(b.maxStaleness)
		} else if time.Now().After(b.staleTime) {
			return fbbatch.ErrBatchNowStale
		}
	}
	return nil
}

func (b *sqlBatch) Import() error {
	if len(b.rows) == 0 {
		return nil
	}

	// Construct the BULK INSERT statement based on the table and fields.
	sql, err := buildBulkInsert(b.table, b.fields, b.ids, b.rows)
	if err != nil {
		return errors.Wrap(err, "building bulk insert statement")
	}

	// Reset batch data.
	b.reset()

	// Submit the SQL statement.
	return b.inserter.Insert(sql)
}

func (b *sqlBatch) reset() {
	b.ids = b.ids[:0]
	b.rows = b.rows[:0]
}

func (b *sqlBatch) Len() int {
	return len(b.rows)
}

func (b *sqlBatch) Flush() error {
	return nil
}

func buildBulkInsert(tbl *dax.Table, fields []*dax.Field, ids []interface{}, rows [][]interface{}) (string, error) {
	// Validation.
	if tbl.Name == "" {
		return "", errors.New(errors.ErrUncoded, "table name is required")
	} else if len(fields) == 0 {
		return "", errors.New(errors.ErrUncoded, "at least one field is required")
	}

	var sb strings.Builder

	sb.WriteString(`BULK INSERT INTO `)
	sb.WriteString(string(tbl.Name))
	sb.WriteString(` (_id,`)

	flds := make([]string, 0, len(fields))
	maps := make([]string, 0, len(fields))
	for i := range fields {
		flds = append(flds, string(fields[i].Name))
		maps = append(maps, fmt.Sprintf("'$.col_%d' %s", i, fields[i].Definition()))
	}
	// Fields
	sb.WriteString(strings.Join(flds, ","))

	// MAP
	keyType := dax.BaseTypeID
	if tbl.StringKeys() {
		keyType = dax.BaseTypeString
	}
	sb.WriteString(`) MAP ('$._id' `)
	sb.WriteString(keyType)
	sb.WriteString(`,`)
	sb.WriteString(strings.Join(maps, ","))
	sb.WriteString(`) FROM x'`)

	// Row values.

	// m is a map representing a single row to be marshalled and added to the
	// bulk insert as one line in the NDJSON payload. We re-use the map for each
	// row.
	m := make(map[string]interface{})
	for i := range rows {
		// Write the ID value.
		m[string(dax.PrimaryKeyFieldName)] = ids[i]
		// Write the rest of the data values.
		for col := range rows[i] {
			m[fmt.Sprintf("col_%d", col)] = rows[i][col]
		}

		// Marshal the map to json and add to the sql statement.
		if j, err := json.Marshal(m); err != nil {
			return "", errors.Wrap(err, "marshalling row to json")
		} else {
			sb.Write(j)
			sb.WriteString("\n")
		}
	}

	// WITH
	sb.WriteString(fmt.Sprintf(`' WITH BATCHSIZE %d FORMAT 'NDJSON' INPUT 'STREAM'`, len(rows)))

	return sb.String(), nil
}
