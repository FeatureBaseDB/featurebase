// Package boltdb contains the boltdb implementation of the Schemar
// interfaces.
package boltdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/mds/schemar"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

var (
	bucketSchemar = boltdb.Bucket("schemar")
)

// SchemarBuckets defines the buckets used by this package. It can be called
// during setup to create the buckets ahead of time.
var SchemarBuckets []boltdb.Bucket = []boltdb.Bucket{
	bucketSchemar,
}

// Ensure type implements interface.
var _ schemar.Schemar = (*Schemar)(nil)

type Schemar struct {
	db *boltdb.DB

	logger logger.Logger
}

// NewSchemar returns a new instance of Schemar with default values.
func NewSchemar(db *boltdb.DB, logger logger.Logger) *Schemar {
	return &Schemar{
		db:     db,
		logger: logger,
	}
}

// CreateTable creates the table provided. If a table with the same name already
// exists then an error is returned.
func (s *Schemar) CreateTable(ctx context.Context, qtbl *dax.QualifiedTable) error {
	// Ensure the table id is not blank.
	if qtbl.ID == "" {
		return schemar.NewErrTableIDInvalid(qtbl.ID)
	}

	// Ensure the table name is not blank.
	if qtbl.Name == "" {
		return schemar.NewErrTableNameInvalid(qtbl.Name)
	}

	// Ensure that a primary key field is present and valid.
	if !qtbl.HasValidPrimaryKey() {
		return schemar.NewErrInvalidPrimaryKey()
	}

	// Set the CreateAt value for the table.
	// TODO(tlt): We may want to consider erroring here if the value is != 0.
	if qtbl.CreatedAt == 0 {
		now := timestamp()
		qtbl.CreatedAt = now

		// Set CreatedAt for all of the fields as well.
		for i := range qtbl.Fields {
			qtbl.Fields[i].CreatedAt = now
		}
	}

	//////////// end validation

	tx, err := s.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "getting transaction")
	}
	defer tx.Rollback()

	// Ensure a table with that ID doesn't already exist.
	if t, _ := s.tableByID(tx, qtbl.TableQualifier, qtbl.ID); t != nil {
		return dax.NewErrTableIDExists(qtbl.QualifiedID())
	}

	if err := s.putTable(tx, qtbl); err != nil {
		return errors.Wrap(err, "putting table")
	}

	// In addition to storing the table in tableKey, we want to store a reverse-lookup
	// (i.e. index) on table name to the tableKey.
	if err := s.putTableName(tx, qtbl); err != nil {
		return errors.Wrap(err, "putting table name")
	}

	return tx.Commit()
}

// CreateField creates the field provided in the given table. If a field with
// the same name already exists then an error is returned.
func (s *Schemar) CreateField(ctx context.Context, qtid dax.QualifiedTableID, fld *dax.Field) error {
	// Ensure the field name is not blank.
	if fld.Name == "" {
		return schemar.NewErrFieldNameInvalid(fld.Name)
	}

	//////////// end validation

	tx, err := s.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "getting transaction")
	}
	defer tx.Rollback()

	// Get the table.
	qtbl, err := s.tableByQTID(tx, qtid)
	if err != nil {
		return errors.Wrap(err, "getting table by id")
	}

	// Ensure a field with that name doesn't already exist.
	if _, ok := qtbl.Field(fld.Name); ok {
		return dax.NewErrFieldExists(fld.Name)
	}

	qtbl.Fields = append(qtbl.Fields, fld)

	// Write table back to database.
	if err := s.putTable(tx, qtbl); err != nil {
		return errors.Wrap(err, "putting table")
	}

	return tx.Commit()
}

// DropField removes the field from the table.
func (s *Schemar) DropField(ctx context.Context, qtid dax.QualifiedTableID, fldName dax.FieldName) error {
	tx, err := s.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "getting transaction")
	}
	defer tx.Rollback()

	// Get the table.
	qtbl, err := s.tableByQTID(tx, qtid)
	if err != nil {
		return errors.Wrap(err, "getting table by id")
	}

	// Ensure a field with that name exists.
	if _, ok := qtbl.Field(fldName); !ok {
		return dax.NewErrFieldDoesNotExist(fldName)
	}

	_ = qtbl.RemoveField(fldName)

	// Write table back to database.
	if err := s.putTable(tx, qtbl); err != nil {
		return errors.Wrap(err, "putting table")
	}

	return tx.Commit()
}

func (s *Schemar) putTable(tx *boltdb.Tx, qtbl *dax.QualifiedTable) error {
	bkt := tx.Bucket(bucketSchemar)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketSchemar)
	}

	val, err := json.Marshal(qtbl)
	if err != nil {
		return errors.Wrap(err, "marshalling table to json")
	}

	return bkt.Put(tableKey(qtbl.OrganizationID, qtbl.DatabaseID, qtbl.Table.ID), val)
}

func (s *Schemar) putTableName(tx *boltdb.Tx, qtbl *dax.QualifiedTable) error {
	bkt := tx.Bucket(bucketSchemar)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketSchemar)
	}

	return bkt.Put(tableNameKey(qtbl.OrganizationID, qtbl.DatabaseID, qtbl.Name), tableKey(qtbl.OrganizationID, qtbl.DatabaseID, qtbl.ID))
}

// Table returns the TableInfo for the given table. An error is returned if the
// table does not exist.
func (s *Schemar) Table(ctx context.Context, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	return s.tableByQTID(tx, qtid)
}

// tableByQTID gets the full qualified table by the QualifiedTableID whether it has Name or ID set.
func (s *Schemar) tableByQTID(tx *boltdb.Tx, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	if qtid.ID == "" {
		return s.tableByName(tx, qtid.TableQualifier, qtid.Name)
	}

	return s.tableByID(tx, qtid.TableQualifier, qtid.ID)
}

func (s *Schemar) tableByName(tx *boltdb.Tx, qual dax.TableQualifier, name dax.TableName) (*dax.QualifiedTable, error) {
	qtid, err := s.tableIDByName(tx, qual, name)
	if err != nil {
		return nil, errors.Wrap(err, "getting table ID")
	}

	return s.tableByID(tx, qtid.TableQualifier, qtid.ID) // TODO remove?
}

func (s *Schemar) tableByID(tx *boltdb.Tx, qual dax.TableQualifier, id dax.TableID) (*dax.QualifiedTable, error) {
	bkt := tx.Bucket(bucketSchemar)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketSchemar)
	}

	b := bkt.Get(tableKey(qual.OrganizationID, qual.DatabaseID, id))
	if b == nil {
		return nil, dax.NewErrTableIDDoesNotExist(dax.QualifiedTableID{TableQualifier: qual, ID: id})
	}

	table := &dax.QualifiedTable{}
	if err := json.Unmarshal(b, table); err != nil {
		return nil, errors.Wrap(err, "unmarshalling table json")
	}

	return table, nil
}

func (s *Schemar) tableIDByName(tx *boltdb.Tx, qual dax.TableQualifier, name dax.TableName) (dax.QualifiedTableID, error) {
	bkt := tx.Bucket(bucketSchemar)
	if bkt == nil {
		return dax.QualifiedTableID{}, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketSchemar)
	}

	b := bkt.Get(tableNameKey(qual.OrganizationID, qual.DatabaseID, name))
	if b == nil {
		return dax.QualifiedTableID{}, dax.NewErrTableNameDoesNotExist(name)
	}

	return keyQualifiedTableID(b)
}

// Tables returns a list of Table for all existing tables. If one or more table
// names is provided, then only those will be included in the output.
func (s *Schemar) Tables(ctx context.Context, qual dax.TableQualifier, ids ...dax.TableID) ([]*dax.QualifiedTable, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	return s.getTables(ctx, tx, qual, ids...)
}

func (s *Schemar) getTables(ctx context.Context, tx *boltdb.Tx, qual dax.TableQualifier, ids ...dax.TableID) (dax.QualifiedTables, error) {
	c := tx.Bucket(bucketSchemar).Cursor()

	// Deserialize rows into Table objects.
	tables := make(dax.QualifiedTables, 0)

	var filterByID bool
	if len(ids) > 0 {
		filterByID = true
	}

	prefix := []byte(fmt.Sprintf(prefixFmtTables, qual.OrganizationID, qual.DatabaseID))
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if v == nil {
			s.logger.Printf("nil value for key: %s", k)
			continue
		}

		tblID, err := keyTableID(k)
		if err != nil {
			return nil, errors.Wrap(err, "getting table from key")
		}

		// Only include tables provided in the ids filter.
		if filterByID && !containsTableID(ids, tblID) {
			continue
		}

		table := &dax.QualifiedTable{}
		if err := json.Unmarshal(v, table); err != nil {
			return nil, errors.Wrap(err, "unmarshalling table json")
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func containsTableID(s []dax.TableID, e dax.TableID) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// DropTable drops the given table. If the named/IDed table does not exist
// then an error is returned.
func (s *Schemar) DropTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	tx, err := s.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "getting transaction")
	}
	defer tx.Rollback()

	// Ensure the table exists.
	qtbl, err := s.tableByQTID(tx, qtid)
	if err != nil {
		return errors.Wrap(err, "getting table by id")
	}

	bkt := tx.Bucket(bucketSchemar)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketSchemar)
	}

	// Delete the table by ID.
	if err := bkt.Delete(tableKey(qtbl.OrganizationID, qtbl.DatabaseID, qtbl.ID)); err != nil {
		return errors.Wrap(err, "deleting table by id")
	}

	// Delete the reverse-lookup table by Name.
	if err := bkt.Delete(tableNameKey(qtbl.OrganizationID, qtbl.DatabaseID, qtbl.Name)); err != nil {
		return errors.Wrap(err, "deleting table by name")
	}

	return tx.Commit()
}

const (
	prefixFmtTables     = "tables/%s/%s/"
	prefixFmtTableNames = "tablenames/%s/%s/"
)

// tableKey returns a key based on a qualified table ID.
func tableKey(orgID dax.OrganizationID, dbID dax.DatabaseID, tblID dax.TableID) []byte {
	key := fmt.Sprintf(prefixFmtTables+"%s", orgID, dbID, tblID)
	return []byte(key)
}

// tableNameKey returns a key based on a qualified table name.
func tableNameKey(orgID dax.OrganizationID, dbID dax.DatabaseID, name dax.TableName) []byte {
	key := fmt.Sprintf(prefixFmtTableNames+"%s", orgID, dbID, name)
	return []byte(key)
}

// keyTableID gets the TableID out of the key.
func keyTableID(key []byte) (dax.TableID, error) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 4 {
		return "", errors.New(errors.ErrUncoded, "table key format expected: `tables/orgID/dbID/tblID`")
	}

	return dax.TableID(parts[3]), nil
}

// keyQualifedTableID gets the QualifiedTableID out of the key.
func keyQualifiedTableID(key []byte) (dax.QualifiedTableID, error) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 4 {
		return dax.QualifiedTableID{}, errors.New(errors.ErrUncoded, "table key format expected: `tables/orgID/dbID/tblID`")
	}

	return dax.NewQualifiedTableID(
		dax.NewTableQualifier(
			dax.OrganizationID(parts[1]),
			dax.DatabaseID(parts[2]),
		),
		dax.TableID(parts[3]),
	), nil
}

func (s *Schemar) TableID(ctx context.Context, qual dax.TableQualifier, name dax.TableName) (dax.QualifiedTableID, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return dax.QualifiedTableID{}, err
	}
	defer tx.Rollback()

	return s.tableIDByName(tx, qual, name)
}

func timestamp() int64 {
	return time.Now().UnixNano()
}
