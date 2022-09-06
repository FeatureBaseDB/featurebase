package sql

import (
	"context"
	"database/sql"
	"io"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
)

type Source struct {
	feed      chan []interface{}
	schema    []idk.Field
	record    wikiRecord
	rows      *sql.Rows
	separator string
	log       logger.Logger
}

func (s *Source) Open() error {
	go func() {
		defer s.rows.Close()
		vals := make([]interface{}, len(s.schema))
		lastRow := make([]interface{}, len(s.schema))

		for i := 0; i < len(vals); i++ {
			vals[i] = new(interface{})
		}
		n := 0
		for s.rows.Next() {
			err := s.rows.Scan(vals...)
			if err != nil {
				s.log.Printf("scanning row '%v'", err)
				continue
			}

			//build message
			row := make([]interface{}, len(vals))
			for i := 0; i < len(vals); i++ {
				item := vals[i].(*interface{})
				switch v := (*item).(type) {
				case string:
					_, isStringArrayField := s.schema[i].(idk.StringArrayField)
					if isStringArrayField {
						row[i] = s.splitStringArray(v)
					} else {
						row[i] = v
					}
				case []byte:
					astring := string(v)
					_, isStringArrayField := s.schema[i].(idk.StringArrayField)
					if isStringArrayField {
						row[i] = s.splitStringArray(astring)
					} else {
						row[i] = astring
					}
				case time.Time:
					row[i] = v
				default:
					row[i] = v

				}
			}
			//skip the dups
			equal := true
			for i, r := range lastRow {
				switch v := r.(type) {
				case []string:
					a, ok := row[i].([]string)
					if len(a) != len(v) {
						equal = false
						break
					}
					if ok {
						for z, o := range v {
							if o != a[z] {
								equal = false
								break

							}

						}

					} else {
						equal = false
						break

					}

				default:
					if v != row[i] {
						equal = false
						break
					}
				}
			}
			n++
			if !equal {
				s.feed <- row
				if (n%10000 == 0 && n < 100000) || (n%100000 == 0) {
					s.log.Printf("Processed: %d", n)
				}
			}
			copy(lastRow, row)
		}
		s.log.Printf("Last %+v", vals)
		if s.rows.Err() != nil {
			s.log.Printf("%v", s.rows.Err())
			return
		}

		s.rows.Close()
		close(s.feed)
	}()
	return nil
}

func (s *Source) Record() (idk.Record, error) {
	vals, ok := <-s.feed
	if ok {
		copy(s.record.record, vals)
		return s.record, nil
	}
	return nil, io.EOF
}

func (s *Source) Schema() []idk.Field {
	return s.schema
}

func NewSource(driver, connString, cmd, separator string, c chan []interface{}, log logger.Logger) (*Source, error) {
	conn, err := sql.Open(driver, connString)
	if err != nil {
		return nil, err
	}
	rows, err := conn.Query(cmd)
	if err != nil {
		return nil, errors.Wrap(err, ": error executing query")
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	schema := make([]idk.Field, len(cols))
	//encoding the type information of the form SELECT col AS col_IDField...
	for i, c := range cols {
		schema[i], err = idk.HeaderToField(c, log)
		if err != nil {
			return nil, errors.Wrapf(err, "making field from '%s'", c)
		}
		if schema[i] == nil {
			return nil, errors.Errorf("nil field for col '%s' but no error", c)
		}
	}
	log.Printf("Schema: %v", schema)
	return &Source{
		feed: c,
		record: wikiRecord{
			record: make([]interface{}, len(cols)),
		},
		schema:    schema,
		rows:      rows,
		separator: separator,
		log:       log,
	}, nil
}

type wikiRecord struct {
	record []interface{}
}

func (wr wikiRecord) Commit(ctx context.Context) error {
	return nil
}

func (wr wikiRecord) Data() []interface{} {
	return wr.record
}

type Main struct {
	idk.Main             `flag:"!embed"`
	Driver               string `help:"key used for finding go sql database driver"`
	ConnectionString     string `short:"g" help:"credentials for connecting to sql database"`
	RowExpr              string `short:"w" help:"sql + type description on input"`
	StringArraySeparator string `help:"separator used to delineate values in string array"`
}

func NewMain() *Main {
	m := &Main{
		Main:                 *idk.NewMain(),
		Driver:               "postgres",
		ConnectionString:     "postgres://user:password@localhost:5432/defaultindex?sslmode=disable",
		StringArraySeparator: ",",
	}
	m.Main.Namespace = "ingester_sql"
	m.NewSource = func() (idk.Source, error) {
		if m.RowExpr == "" {
			return nil, errors.New("must provide a Row Expression")
		}
		source, err := NewSource(m.Driver, m.ConnectionString, m.RowExpr, m.StringArraySeparator, make(chan []interface{}), m.Log())
		if err != nil {
			return nil, errors.Wrap(err, "creating source")
		}
		err = source.Open()
		if err != nil {
			return nil, errors.Wrap(err, "opening source")
		}
		return source, nil
	}
	return m
}

// splitIfStringArray splits `astring` by each separator in s.separator
func (s Source) splitStringArray(astring string) []string {
	return strings.FieldsFunc(astring, Splitter(s.separator))
}

// Splitter returns a splitter based on runes in a separator
func Splitter(separator string) func(rune) bool {
	return func(r rune) bool {
		for _, val := range separator {
			if r == val {
				return true
			}
		}
		return false
	}
}

func (s *Source) Close() error {
	return nil
}
