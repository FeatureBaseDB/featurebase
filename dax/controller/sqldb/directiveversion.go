package sqldb

import (
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

func NewDirectiveVersion(log logger.Logger) dax.DirectiveVersion {
	if log == nil {
		log = logger.NopLogger
	}
	return &directiveVersion{
		log: log,
	}
}

type directiveVersion struct {
	log logger.Logger
}

func (d *directiveVersion) GetCurrent(tx dax.Transaction, addr dax.Address) (uint64, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return 0, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}
	dv := &models.DirectiveVersion{}
	err := dt.C.Find(dv, addr)
	if err == nil {
		return uint64(dv.Version), nil
	}

	// If there is not yet a record for address, create one and return 0 as the
	// "current version".
	if err.Error() == "sql: no rows in result set" {
		dv.ID = string(addr)
		if err := dt.C.Create(dv); err != nil {
			return 0, errors.Wrapf(err, "creating directive_version for address: %s", addr)
		}
		return 0, nil
	}

	return 0, errors.Wrapf(err, "finding directive_version for address: %s", addr)
}

func (d *directiveVersion) SetNext(tx dax.Transaction, addr dax.Address, current, next uint64) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	dv := &models.DirectiveVersion{}

	// Table is assumed to be pre-populated by a previous call to GetCurrent. We
	// use the postgres specific "RETURNING" along with `.First()` to ensure
	// that a record was updated. If no record matches the WHERE clause, then
	// RETURNING would return a result set with 0 records, which causes
	// `.First()` to return an error.
	err := dt.C.RawQuery(`
		UPDATE directive_versions
		SET version = ?, updated_at = NOW()
		WHERE id = ?
		AND version = ?
		RETURNING id, version`, next, addr, current).First(dv)
	if err != nil {
		return errors.Wrapf(err, "updating directive_version for address: %s", addr)
	}
	return nil
}
