package sqldb

import "strings"

func isNoRowsError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no rows in result set")
}

func isViolatesUniqueConstraint(err error) bool {
	return err != nil && strings.Contains(err.Error(), "duplicate key value violates unique constraint")
}
