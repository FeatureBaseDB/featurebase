package sqldb

import "strings"

func isNoRowsError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no rows in result set")
}
