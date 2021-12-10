package sql

import (
	"strings"

	"github.com/molecula/featurebase/v2/logger"
	"github.com/pkg/errors"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	SQLTypeSelect = "select"
	SQLTypeShow   = "show"
	SQLTypeSet    = "set"
	SQLTypeBegin  = "begin"
	SQLTypeEmpty  = ""
)

// System errors.
var (
	ErrMultipleSQLStatements = errors.New("statement contains multiple sql queries")
)

type MappedSQL struct {
	SQLType   string
	Statement sqlparser.Statement
	Mask      QueryMask
	Tables    []string
	SQL       string
}

// Mapper is responsible for mapping a SQL query to structure representation
type Mapper struct {
	Logger logger.Logger
}

func NewMapper() *Mapper {
	return &Mapper{
		Logger: logger.NopLogger,
	}
}

// Parse parses SQL query
func (m *Mapper) Parse(sql string) (sqlparser.Statement, QueryMask, error) {
	parsed, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, QueryMask{}, errors.Wrap(err, "parsing sql")
	}

	qm := GenerateMask(parsed)

	return parsed, qm, nil
}

// MapSQL converts a sql string into a MappedSQL object,
// which includes the parsed query and the query mask,
// among other information about the query.
func (m *Mapper) MapSQL(sql string) (*MappedSQL, error) {
	// In the case where `sql` contains more than one query—since
	// we don't support multiple return sets—we're going to just
	// ignore everything and return a specific error type. This
	// will allow the caller to handle it as needed (i.e. it can
	// return the error, or return an empty result set).
	if parts := strings.Split(sql, ";"); len(parts) > 1 {
		var partCount int
		for _, part := range parts {
			if trimmed := strings.TrimSpace(part); trimmed != "" && trimmed != "\x00" {
				partCount++
			}
		}
		if partCount != 1 {
			return nil, ErrMultipleSQLStatements
		}
	}

	stmt, qm, err := m.Parse(sql)
	if err != nil {
		return nil, errors.Wrap(err, "parsing sql")
	}

	var sqlType string
	var tableNames []string
	switch slct := stmt.(type) {
	case *sqlparser.Select:
		sqlType = SQLTypeSelect
		tableNames, err = extractTableNames(slct)
		if err != nil {
			return nil, errors.Wrap(err, "extracting table names")
		}
	case *sqlparser.Show:
		sqlType = SQLTypeShow
	case *sqlparser.Set:
		sqlType = SQLTypeSet
	case *sqlparser.Begin:
		sqlType = SQLTypeBegin
	}

	return &MappedSQL{
		SQLType:   sqlType,
		Statement: stmt,
		Mask:      qm,
		Tables:    tableNames,
		SQL:       sql,
	}, nil
}
