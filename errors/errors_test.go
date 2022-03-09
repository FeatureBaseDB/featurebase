package errors_test

import (
	"fmt"
	"testing"

	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/stretchr/testify/assert"
)

func TestErrors(t *testing.T) {
	t.Run("Is", func(t *testing.T) {
		uncoded := newUncoded("uncoded error")
		fnf := newErrFieldNotFound("fld")
		tnf := newErrTableNotFound("tbl")
		fnfCustom := errors.New(errFieldNotFound, "custom field message")

		tests := []struct {
			err    error
			target errors.Code
			exp    bool
		}{
			{
				err:    uncoded,
				target: errUncoded,
				exp:    true,
			},
			{
				err:    uncoded,
				target: errFieldNotFound,
				exp:    false,
			},
			{
				err:    fnf,
				target: errFieldNotFound,
				exp:    true,
			},
			{
				err:    fnf,
				target: errTableNotFound,
				exp:    false,
			},
			{
				err:    errors.Wrap(tnf, "with message"),
				target: errTableNotFound,
				exp:    true,
			},
			{
				err:    fnfCustom,
				target: errFieldNotFound,
				exp:    true,
			},
		}

		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				got := errors.Is(test.err, test.target)
				assert.Equal(t, test.exp, got)
			})
		}
	})
}

// Test error codes.

const (
	errUncoded       errors.Code = "Uncoded"
	errFieldNotFound errors.Code = "FieldNotFound"
	errTableNotFound errors.Code = "TableNotFound"
)

func newUncoded(message string) error {
	return errors.New(
		errUncoded,
		message,
	)
}

func newErrFieldNotFound(field string) error {
	return errors.New(
		errFieldNotFound,
		"field not found: "+field,
	)
}

func newErrTableNotFound(table string) error {
	return errors.New(
		errTableNotFound,
		"table not found: "+table,
	)
}
