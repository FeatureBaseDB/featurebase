package idk

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/featurebasedb/featurebase/v3/logger"
)

const IDColumn = "id"

type PostgresClient struct {
	DSN         string
	TableName   string
	ColumnNames []string
	client      *sql.DB
	log         logger.Logger
}

// NewPostgresClient creates a Lookup client for a Postgres backend.
// It creates the corresponding Postgres table if necessary.
func NewPostgresClient(DSN, TableName string, log logger.Logger) (*PostgresClient, error) {
	c := PostgresClient{
		DSN: DSN,
		log: log,
	}

	db, err := sql.Open("postgres", DSN)
	if err != nil {
		return &c, errors.Wrap(err, "creating Postgres client")
	}
	c.client = db
	c.TableName = TableName

	// Create table with no data columns once, because
	// postgres has a problem with concurrent identical table creates.
	err = c.createTableWithTextColumns([]string{})
	if err != nil {
		return &c, errors.Wrap(err, "initializing Postgres table")
	}

	return &c, nil
}

// selectLookupFieldNames returns the list of fields from the Source's schema
// which are of type LookupTextField.
func selectLookupFieldNames(source Source) []string {
	columnNames := make([]string, 0)
	for _, field := range source.Schema() {
		// NOTE support other data types here; columnNames as map[fieldType]string instead of []string
		switch field.(type) {
		case LookupTextField:
			columnNames = append(columnNames, field.Name())
		default:
			continue
		}
	}
	return columnNames
}

// Setup primarily serves to add text columns to the Postgres table.
// Column names are not known until ingester.Run starts,
// so columns are created separately from table creation.
func (c *PostgresClient) Setup(columnNames []string) error {
	for _, name := range columnNames {
		if name == IDColumn {
			return errors.Errorf("field name '%s' not allowed for LookupText fields", IDColumn)
		}
	}
	c.ColumnNames = make([]string, 0, len(columnNames)+1)
	c.ColumnNames = append(c.ColumnNames, IDColumn)
	c.ColumnNames = append(c.ColumnNames, columnNames...)

	return c.addTextColumns(c.ColumnNames[1:])
}

// addTextColumns adds columns of type "text NOT NULL" to the Postgres table, with the specified names.
func (c *PostgresClient) addTextColumns(columnNames []string) error {
	if len(columnNames) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := c.client.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "creating transaction")
	}

	var committed bool
	defer func() {
		if !committed {
			if err := tx.Rollback(); err != nil {
				panic(err)
			}
		}
	}()

	columnStrings := make([]string, 0)
	for _, fname := range columnNames {
		// NOTE support other data types here
		columnStrings = append(columnStrings, fmt.Sprintf(`ADD COLUMN IF NOT EXISTS "%s" text NOT NULL`, fname))
	}
	sqlString := fmt.Sprintf(`ALTER TABLE "%s" %s`,
		c.TableName,
		strings.Join(columnStrings, ", "),
	)
	if _, err := tx.Exec(sqlString); err != nil {
		return errors.Wrapf(err, "altering postgres table %s", c.TableName)
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "commiting postgres table alter")
	}
	c.log.Debugf("added postgres columns to table '%s'", c.TableName)

	committed = true
	return nil
}

// createTableWithTextColumns creates a table with:
// - the name specified in the constructor,
// - an id column with hardcoded name "id", of type "bigint PRIMARY KEY",
// - any number of data columns of type "text NOT NULL" with the specified names.
func (c *PostgresClient) createTableWithTextColumns(columnNames []string) error {
	var ok bool
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := c.client.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "creating transaction")
	}

	defer func() {
		if !ok {
			rerr := tx.Rollback()
			if rerr != nil {
				panic(rerr)
			}
		} else {
			c.log.Debugf("created postgres table '%s'", c.TableName)
		}
	}()

	// NOTE support string IDs in postgres here

	columnStrings := make([]string, 0)
	columnStrings = append(columnStrings, IDColumn+" bigint PRIMARY KEY")
	for _, fname := range columnNames {
		// NOTE support other data types here
		columnStrings = append(columnStrings, fmt.Sprintf(`"%s" text NOT NULL`, fname))
	}
	sqlString := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s)`,
		c.TableName,
		strings.Join(columnStrings, ", "),
	)
	_, err = tx.Exec(sqlString)
	if err != nil {
		return errors.Wrapf(err, "creating postgres table %s", c.TableName)
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "commiting postgres table create")
	}
	ok = true
	return nil
}

func (c *PostgresClient) Close() error {
	return c.client.Close()
}

type LookupBatcher interface {
	AddFullRow([]interface{}) error
	Len() int
	Import() error
}

var _ LookupBatcher = &NopLookupBatcher{}
var _ LookupBatcher = &PostgresUpsertBatcher{}

// NopLookupBatcher is a no-op implementation of a LookupBatcher.
type NopLookupBatcher struct{}

func (n *NopLookupBatcher) Len() int                           { return 0 }
func (n *NopLookupBatcher) AddFullRow(row []interface{}) error { return nil }
func (n *NopLookupBatcher) Import() error                      { return nil }

// PostgresUpsertBatcher does bulk imports using an INSERT ... ON CONFLICT statement,
// to handle duplicate-key updates.
type PostgresUpsertBatcher struct {
	client      *sql.DB
	batchSize   int
	tableName   string
	columnNames []string

	rowCount  int
	valSlice  []interface{} // 1D slice of all rows' values, for variadic Exec
	startTime time.Time

	// static strings for building SQL
	columnNamesAsString string   // `"column-1", "column2"`
	conflictString      string   // `"column-1"=EXCLUDED."column-1", "column2"=EXCLUDED."column2"`
	rowPlaceholders     []string // [`($1,$2)` `($3,$4)` `($5,$6)` `($7,$8)` `($9,$10)`] (each of the batchSize elements has len(ColumnNames) placeholders)

	log logger.Logger
}

// NewPostgresUpsertBatcher creates a LookupBatcher, with a Postgres backend,
// which uses the Upsert method (see the Import function for details) to batch imports.
func NewPostgresUpsertBatcher(client *PostgresClient, batchSize int) *PostgresUpsertBatcher {
	pg := &PostgresUpsertBatcher{
		client:      client.client,
		batchSize:   batchSize,
		tableName:   client.TableName,
		columnNames: client.ColumnNames,
		log:         client.log,
	}
	pg.valSlice = make([]interface{}, 0, pg.batchSize*len(pg.columnNames))

	quotedNames := make([]string, 0, len(pg.columnNames))
	for _, name := range pg.columnNames {
		quotedNames = append(quotedNames, `"`+name+`"`)
	}
	pg.columnNamesAsString = strings.Join(quotedNames, ", ")

	conflictStrings := []string{}
	for _, name := range pg.columnNames {
		conflictString := fmt.Sprintf(`"%s"=EXCLUDED."%s"`, name, name)
		conflictStrings = append(conflictStrings, conflictString)
	}
	pg.conflictString = strings.Join(conflictStrings, ", ")

	pg.rowPlaceholders = make([]string, 0, pg.batchSize)
	placeholders := make([]string, len(pg.columnNames))
	// placeholders = [`$1` `$2`]
	// pg.rowPlaceHolders = [`($1, $2)` `($3, $4)` ...]
	for i := 0; i < pg.batchSize; i++ {
		for j := 0; j < len(pg.columnNames); j++ {
			placeholders[j] = fmt.Sprintf("$%d", i*len(pg.columnNames)+j+1)
		}
		pg.rowPlaceholders = append(pg.rowPlaceholders, "("+strings.Join(placeholders, ",")+")")
	}

	return pg
}

// Len returns the number of rows in the current batch, that have yet to be imported
// to the Postgres backend.
func (pg *PostgresUpsertBatcher) Len() int {
	return pg.rowCount
}

// AddFullRow adds a row to the batch, which includes data for each column in the
// table. If the addition causes the batch to reach the predefined batch size,
// an import occurs.
// An alternative might be AddPartialRow, which accepts data for only some
// of the columns in the table. An AddPartialRow function would be able to handle
// any number of missing Lookup fields in received messages; AddFullRow can not.
func (pg *PostgresUpsertBatcher) AddFullRow(row []interface{}) error {
	if pg.rowCount == 0 {
		pg.startTime = time.Now()
	}

	pg.valSlice = append(pg.valSlice, row...)
	pg.rowCount++

	if pg.rowCount == pg.batchSize {
		err := pg.Import()
		if err != nil {
			return errors.Wrap(err, "flushing postgres batch")
		}
	}

	return nil
}

// Import sends a batch of row data to Postgres using an INSERT ... ON CONFLICT statement.
// The ON CONFLICT portion has no effect on the rows in the batch with new ids. For rows with
// ids that already exist in the Postgres table, the ON CONFLICT causes the new values for the
// data columns to overwrite the existing values.
func (pg *PostgresUpsertBatcher) Import() error {
	if pg.rowCount == 0 {
		pg.log.Debugf("skipped empty postgres import")
		return nil
	}

	allPlaceholders := strings.Join(pg.rowPlaceholders[:pg.rowCount], ",")

	// INSERT INTO "lookup_test" (id, rawlog)
	// VALUES (8, 'foo')
	// ON CONFLICT (id) DO UPDATE SET rawlog=EXCLUDED.rawlog;
	sql := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES %s ON CONFLICT ("%s") DO UPDATE SET %s;`,
		pg.tableName,
		pg.columnNamesAsString,
		allPlaceholders,
		IDColumn,
		pg.conflictString,
	)

	tx, err := pg.client.Begin()
	if err != nil {
		return errors.Wrap(err, "creating postgres transaction")
	}

	var committed bool
	defer func() {
		if !committed {
			if err := tx.Rollback(); err != nil {
				panic(err)
			}
		}
	}()

	if _, err := tx.Exec(sql, pg.valSlice...); err != nil {
		return errors.Wrap(err, "executing postgres import query")
	}

	if err = tx.Commit(); err != nil {
		return errors.Wrap(err, "committing postgres transaction")
	}
	pg.log.Printf("imported Postgres batch of %d rows after %v\n", pg.rowCount, time.Since(pg.startTime))

	pg.valSlice = pg.valSlice[:0]
	pg.rowCount = 0
	committed = true

	return nil
}
