package sqldb

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/models"
)

var _ dax.DirectiveVersion = (*DirectiveVersion)(nil)

type DirectiveVersion struct{}

func (d *DirectiveVersion) Increment(tx dax.Transaction, delta uint64) (uint64, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return 0, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}
	// table is pre-populated w/ a single record w/ ID=1 during schema migration
	dv := &models.DirectiveVersion{}
	err := dt.C.RawQuery("UPDATE directive_versions SET version = version + ? WHERE id = ? RETURNING id, version", delta, 1).First(dv)
	if err != nil {
		fmt.Println(err)
	}
	return uint64(dv.Version), nil
}
