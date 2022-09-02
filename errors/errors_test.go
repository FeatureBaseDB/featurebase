package errors_test

import (
	"fmt"
	"testing"

	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/stretchr/testify/assert"
)

func TestErrors(t *testing.T) {

	var errUncoded errors.Code = "TestErrUncoded"
	var errFieldNotFound errors.Code = "TestErrFieldNotFound"
	var errTableNotFound errors.Code = "TestErrTableNotFound"

	newErrFieldNotFound := func(fld string) error {
		return errors.New(
			errFieldNotFound,
			fmt.Sprintf("field not found '%s'", fld),
		)
	}

	newErrTableNotFound := func(tbl string) error {
		return errors.New(
			errTableNotFound,
			fmt.Sprintf("table not found '%s'", tbl),
		)
	}

	t.Run("Is", func(t *testing.T) {
		uncoded := errors.New(errUncoded, "uncoded error")
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
