package sql

import (
	"strings"
	"time"
)

const ColID = "_id"

type FuncName string

const (
	FuncCount FuncName = "count"
	FuncMin   FuncName = "min"
	FuncMax   FuncName = "max"
	FuncSum   FuncName = "sum"
	FuncAvg   FuncName = "avg"
)

// Column is an interface which supports mapping to
// source headers, and aliasing column names.
// Alias() should always return a value;
// either a unique alias, or the same value
// return by Name(), but never an empty string.
type Column interface {
	Source() string
	Name() string
	Alias() string
}

type BasicColumn struct {
	source string
	name   string
	alias  string
}

func NewBasicColumn(s, n, a string) *BasicColumn {
	return &BasicColumn{
		source: s,
		name:   n,
		alias:  a,
	}
}

func (b *BasicColumn) Source() string {
	return b.source
}
func (b *BasicColumn) Name() string {
	return b.name
}
func (b *BasicColumn) Alias() string {
	if b.alias != "" {
		return b.alias
	}
	return b.name
}

type StarColumn struct{}

func NewStarColumn() *StarColumn {
	return &StarColumn{}
}

func (s *StarColumn) Source() string {
	return ""
}
func (s *StarColumn) Name() string {
	return ""
}
func (s *StarColumn) Alias() string {
	return ""
}

func ConvertToTime(text string) (time.Time, bool) {
	timeFormats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	}
	for _, timeFormat := range timeFormats {
		if t, err := time.Parse(timeFormat, text); err == nil {
			return t, true
		}
	}
	return time.Now(), false
}

func ExtractFieldName(columName string) (fieldName string, isSpecial bool) {
	isSpecial = false
	fieldName = columName
	if strings.HasPrefix(columName, "_") {
		isSpecial = true
		if strings.HasSuffix(columName, "_time") {
			fieldName = columName[1 : len(columName)-5]
		}
	}
	return
}
