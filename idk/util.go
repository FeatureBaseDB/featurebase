package idk

import (
	"github.com/featurebasedb/featurebase/v3/pql"
)

func scaledStringToInt(scale int64, num string) (int64, error) {
	d, err := pql.ParseDecimal(num)
	if err != nil {
		return 0, err
	}
	return d.ToInt64(scale), nil
}
