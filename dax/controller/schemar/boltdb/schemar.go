// Package boltdb contains the boltdb implementation of the Schemar
// interfaces.
package boltdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/controller/schemar"
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

// CreateDatabase creates the database provided. If a database with the same
// name already exists then an error is returned. For now, we are not going to
// store the tables in the schemar Database struct.
func (s *Schemar) CreateDatabase(tx dax.Transaction, qdb *dax.QualifiedDatabase) error {
	// Ensure the database id is not blank.
	if qdb.ID == "" {
		return schemar.NewErrDatabaseIDInvalid(qdb.ID)
	}

	// Ensure the database name is not blank.
	if qdb.Name == "" {
		return schemar.NewErrDatabaseNameInvalid(qdb.Name)
	}

	// Set the CreateAt value for the database.
	// TODO(tlt): We may want to consider erroring here if the value is != 0.
	if qdb.CreatedAt == 0 {
		now := timestamp()
		qdb.CreatedAt = now
	}

	//////////// end validation

	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction()
	}

	// Ensure a database with that ID doesn't already exist.
	if db, _ := s.databaseByID(txx, qdb.OrganizationID, qdb.ID); db != nil {
		return dax.NewErrDatabaseIDExists(qdb.QualifiedID())
	}

	if err := s.putDatabase(txx, qdb); err != nil {
		return errors.Wrap(err, "putting database")
	}

	// In addition to storing the database in databaseKey, we want to store a
	// reverse-lookup (i.e. index) on database name to the databaseKey.
	if err := s.putDatabaseName(txx, qdb); err != nil {
		return errors.Wrap(err, "putting database name")
	}

	return nil
}

func (s *Schemar) DatabaseByID(tx dax.Transaction, qdbid dax.QualifiedDatabaseID) (*dax.QualifiedDatabase, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction()
	}

	return s.databaseByID(txx, qdbid.OrganizationID, qdbid.DatabaseID)
}

func (s *Schemar) databaseByID(tx *boltdb.Tx, orgID dax.OrganizationID, id dax.DatabaseID) (*dax.QualifiedDatabase, error) {
	bkt := tx.Bucket(bucketSchemar)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketSchemar)
	}

	b := bkt.Get(databaseKey(orgID, id))
	if b == nil {
		return nil, dax.NewErrDatabaseIDDoesNotExist(dax.QualifiedDatabaseID{OrganizationID: orgID, DatabaseID: id})
	}

	database := &dax.QualifiedDatabase{}
	if err := json.Unmarshal(b, database); err != nil {
		return nil, errors.Wrap(err, "unmarshalling database json")
	}

	return database, nil
}

func (s *Schemar) DatabaseByName(tx dax.Transaction, orgID dax.OrganizationID, dbname dax.DatabaseName) (*dax.QualifiedDatabase, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction()
	}

	return s.databaseByName(txx, orgID, dbname)
}

func (s *Schemar) databaseByName(tx *boltdb.Tx, orgID dax.OrganizationID, name dax.DatabaseName) (*dax.QualifiedDatabase, error) {
	qdbid, err := s.databaseIDByName(tx, orgID, name)
	if err != nil {
		return nil, errors.Wrap(err, "getting database ID")
	}

	return s.databaseByID(tx, orgID, qdbid.DatabaseID)
}

func (s *Schemar) databaseIDByName(tx *boltdb.Tx, orgID dax.OrganizationID, name dax.DatabaseName) (dax.QualifiedDatabaseID, error) {
	bkt := tx.Bucket(bucketSchemar)
	if bkt == nil {
		return dax.QualifiedDatabaseID{}, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketSchemar)
	}

	b := bkt.Get(databaseNameKey(orgID, name))
	if b == nil {
		return dax.QualifiedDatabaseID{}, dax.NewErrDatabaseNameDoesNotExist(name)
	}

	return keyQualifiedDatabaseID(b)
}

func (s *Schemar) putDatabase(tx *boltdb.Tx, qdb *dax.QualifiedDatabase) error {
	bkt := tx.Bucket(bucketSchemar)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketSchemar)
	}

	val, err := json.Marshal(qdb)
	if err != nil {
		return errors.Wrap(err, "marshalling database to json")
	}

	return bkt.Put(databaseKey(qdb.OrganizationID, qdb.ID), val)
}

func (s *Schemar) putDatabaseName(tx *boltdb.Tx, qdb *dax.QualifiedDatabase) error {
	bkt := tx.Bucket(bucketSchemar)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketSchemar)
	}

	return bkt.Put(databaseNameKey(qdb.OrganizationID, qdb.Name), databaseKey(qdb.OrganizationID, qdb.ID))
}

// DropDatabase drops the given database. If the named/IDed database does not
// exist then an error is returned.
func (s *Schemar) DropDatabase(tx dax.Transaction, qdbid dax.QualifiedDatabaseID) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction()
	}

	// Ensure the database exists.
	qdb, err := s.databaseByID(txx, qdbid.OrganizationID, qdbid.DatabaseID)
	if err != nil {
		return errors.Wrap(err, "getting database by id")
	}

	bkt := txx.Bucket(bucketSchemar)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketSchemar)
	}

	// Delete the database by ID.
	if err := bkt.Delete(databaseKey(qdb.OrganizationID, qdb.ID)); err != nil {
		return errors.Wrap(err, "deleting database by id")
	}

	// Delete the reverse-lookup database by Name.
	if err := bkt.Delete(databaseNameKey(qdb.OrganizationID, qdb.Name)); err != nil {
		return errors.Wrap(err, "deleting database by name")
	}

	return nil
}

// SetDatabaseOptions overwrites the existing database options with those
// provided for the given database.
func (s *Schemar) SetDatabaseOptions(tx dax.Transaction, qdbid dax.QualifiedDatabaseID, opts dax.DatabaseOptions) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction()
	}

	// Get the database.
	qdb, err := s.databaseByID(txx, qdbid.OrganizationID, qdbid.DatabaseID)
	if err != nil {
		return errors.Wrapf(err, "getting database: %s", qdbid)
	}

	// Set the new options.
	qdb.Options = opts

	// Put the database.
	if err := s.putDatabase(txx, qdb); err != nil {
		return errors.Wrap(err, "putting database")
	}

	return nil
}

func (s *Schemar) Databases(tx dax.Transaction, orgID dax.OrganizationID, ids ...dax.DatabaseID) ([]*dax.QualifiedDatabase, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction()
	}

	return s.getDatabases(txx, orgID, ids...)
}

func (s *Schemar) getDatabases(tx *boltdb.Tx, orgID dax.OrganizationID, ids ...dax.DatabaseID) (dax.QualifiedDatabases, error) {
	c := tx.Bucket(bucketSchemar).Cursor()

	// Deserialize rows into Database objects.
	databases := make(dax.QualifiedDatabases, 0)

	var filterByID bool
	if len(ids) > 0 {
		filterByID = true
	}

	prefix := []byte(fmt.Sprintf(prefixFmtDatabases, orgID))
	if orgID == "" {
		prefix = []byte(prefixDatabases)
	}

	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if v == nil {
			s.logger.Printf("nil value for key: %s", k)
			continue
		}

		dbID, err := keyDatabaseID(k)
		if err != nil {
			return nil, errors.Wrap(err, "getting database from key")
		}

		// Only include databases provided in the ids filter.
		if filterByID && !containsDatabaseID(ids, dbID) {
			continue
		}

		database := &dax.QualifiedDatabase{}
		if err := json.Unmarshal(v, database); err != nil {
			return nil, errors.Wrap(err, "unmarshalling database json")
		}

		databases = append(databases, database)
	}

	return databases, nil
}

func containsDatabaseID(s []dax.DatabaseID, e dax.DatabaseID) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// CreateTable creates the table provided. If a table with the same name already
// exists then an error is returned.
func (s *Schemar) CreateTable(tx dax.Transaction, qtbl *dax.QualifiedTable) error {
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

	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction()
	}

	// Ensure the database, defined in the table's QualifiedDatabaseID, exists.
	if _, err := s.databaseByID(txx, qtbl.OrganizationID, qtbl.DatabaseID); err != nil {
		return errors.Wrap(err, "validating database")
	}

	// Ensure a table with that ID doesn't already exist.
	if t, _ := s.tableByID(txx, qtbl.QualifiedDatabaseID, qtbl.ID); t != nil {
		return dax.NewErrTableIDExists(qtbl.QualifiedID())
	}

	if err := s.putTable(txx, qtbl); err != nil {
		return errors.Wrap(err, "putting table")
	}

	// In addition to storing the table in tableKey, we want to store a reverse-lookup
	// (i.e. index) on table name to the tableKey.
	if err := s.putTableName(txx, qtbl); err != nil {
		return errors.Wrap(err, "putting table name")
	}

	return nil
}

// CreateField creates the field provided in the given table. If a field with
// the same name already exists then an error is returned.
func (s *Schemar) CreateField(tx dax.Transaction, qtid dax.QualifiedTableID, fld *dax.Field) error {
	// Ensure the field name is not blank.
	if fld.Name == "" {
		return schemar.NewErrFieldNameInvalid(fld.Name)
	}

	//////////// end validation

	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction()
	}

	// Get the table.
	qtbl, err := s.tableByQTID(txx, qtid)
	if err != nil {
		return errors.Wrap(err, "getting table by id")
	}

	// Ensure a field with that name doesn't already exist.
	if _, ok := qtbl.Field(fld.Name); ok {
		return dax.NewErrFieldExists(fld.Name)
	}

	qtbl.Fields = append(qtbl.Fields, fld)

	// Write table back to database.
	if err := s.putTable(txx, qtbl); err != nil {
		return errors.Wrap(err, "putting table")
	}

	return nil
}

// DropField removes the field from the table.
func (s *Schemar) DropField(tx dax.Transaction, qtid dax.QualifiedTableID, fldName dax.FieldName) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction()
	}

	// Get the table.
	qtbl, err := s.tableByQTID(txx, qtid)
	if err != nil {
		return errors.Wrap(err, "getting table by id")
	}

	// Ensure a field with that name exists.
	if _, ok := qtbl.Field(fldName); !ok {
		return dax.NewErrFieldDoesNotExist(fldName)
	}

	_ = qtbl.RemoveField(fldName)

	// Write table back to database.
	if err := s.putTable(txx, qtbl); err != nil {
		return errors.Wrap(err, "putting table")
	}

	return nil
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
func (s *Schemar) Table(tx dax.Transaction, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction()
	}

	return s.tableByQTID(txx, qtid)
}

// tableByQTID gets the full qualified table by the QualifiedTableID whether it
// has Name or ID set.
func (s *Schemar) tableByQTID(tx *boltdb.Tx, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	if qtid.ID == "" {
		return s.tableByName(tx, qtid.QualifiedDatabaseID, qtid.Name)
	}

	return s.tableByID(tx, qtid.QualifiedDatabaseID, qtid.ID)
}

func (s *Schemar) tableByName(tx *boltdb.Tx, qdbid dax.QualifiedDatabaseID, name dax.TableName) (*dax.QualifiedTable, error) {
	qtid, err := s.tableIDByName(tx, qdbid, name)
	if err != nil {
		return nil, errors.Wrap(err, "getting table ID")
	}

	return s.tableByID(tx, qtid.QualifiedDatabaseID, qtid.ID) // TODO remove?
}

func (s *Schemar) tableByID(tx *boltdb.Tx, qdbid dax.QualifiedDatabaseID, id dax.TableID) (*dax.QualifiedTable, error) {
	bkt := tx.Bucket(bucketSchemar)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketSchemar)
	}

	b := bkt.Get(tableKey(qdbid.OrganizationID, qdbid.DatabaseID, id))
	if b == nil {
		return nil, dax.NewErrTableIDDoesNotExist(dax.QualifiedTableID{QualifiedDatabaseID: qdbid, ID: id})
	}

	table := &dax.QualifiedTable{}
	if err := json.Unmarshal(b, table); err != nil {
		return nil, errors.Wrap(err, "unmarshalling table json")
	}

	return table, nil
}

func (s *Schemar) tableIDByName(tx *boltdb.Tx, qdbid dax.QualifiedDatabaseID, name dax.TableName) (dax.QualifiedTableID, error) {
	bkt := tx.Bucket(bucketSchemar)
	if bkt == nil {
		return dax.QualifiedTableID{}, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketSchemar)
	}

	b := bkt.Get(tableNameKey(qdbid.OrganizationID, qdbid.DatabaseID, name))
	if b == nil {
		return dax.QualifiedTableID{}, dax.NewErrTableNameDoesNotExist(name)
	}

	return keyQualifiedTableID(b)
}

// Tables returns a list of Table for all existing tables. If one or more table
// IDs is provided, then only those will be included in the output.
func (s *Schemar) Tables(tx dax.Transaction, qdbid dax.QualifiedDatabaseID, ids ...dax.TableID) ([]*dax.QualifiedTable, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction()
	}

	return s.getTables(txx, qdbid, ids...)
}

func (s *Schemar) getTables(tx *boltdb.Tx, qdbid dax.QualifiedDatabaseID, ids ...dax.TableID) (dax.QualifiedTables, error) {
	c := tx.Bucket(bucketSchemar).Cursor()

	// Deserialize rows into Table objects.
	tables := make(dax.QualifiedTables, 0)

	var filterByID bool
	if len(ids) > 0 {
		filterByID = true
	}

	prefix := []byte(fmt.Sprintf(prefixFmtTables, qdbid.OrganizationID, qdbid.DatabaseID))
	if qdbid.OrganizationID == "" && qdbid.DatabaseID == "" {
		prefix = []byte(prefixTables)
	} else if qdbid.DatabaseID == "" {
		prefix = []byte(fmt.Sprintf(prefixFmtTablesOrg, qdbid.OrganizationID))
	}

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
func (s *Schemar) DropTable(tx dax.Transaction, qtid dax.QualifiedTableID) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction()
	}

	// Ensure the table exists.
	qtbl, err := s.tableByQTID(txx, qtid)
	if err != nil {
		return errors.Wrap(err, "getting table by id")
	}

	bkt := txx.Bucket(bucketSchemar)
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

	return nil
}

func (s *Schemar) TableID(tx dax.Transaction, qdbid dax.QualifiedDatabaseID, name dax.TableName) (dax.QualifiedTableID, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.QualifiedTableID{}, dax.NewErrInvalidTransaction()
	}

	return s.tableIDByName(txx, qdbid, name)
}

const (
	prefixTables        = "tables/"
	prefixFmtTablesOrg  = prefixTables + "%s/"       // org-id
	prefixFmtTables     = prefixFmtTablesOrg + "%s/" // db-id
	prefixFmtTableNames = "tablenames/%s/%s/"        // org-id, db-id

	prefixDatabases        = "databases/"
	prefixFmtDatabases     = prefixDatabases + "%s/"   // org-id
	prefixFmtDatabase      = prefixFmtDatabases + "%s" // db-id
	prefixFmtDatabaseNames = "databasenames/%s/"       // org-id
)

// databaseKey returns a key based on a qualified database ID.
func databaseKey(orgID dax.OrganizationID, dbID dax.DatabaseID) []byte {
	key := fmt.Sprintf(prefixFmtDatabase, orgID, dbID)
	return []byte(key)
}

// databaseNameKey returns a key based on a qualified database name.
func databaseNameKey(orgID dax.OrganizationID, name dax.DatabaseName) []byte {
	key := fmt.Sprintf(prefixFmtDatabaseNames+"%s", orgID, name)
	return []byte(key)
}

// keyDatabaseID gets the DatabaseID out of the key.
func keyDatabaseID(key []byte) (dax.DatabaseID, error) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 3 {
		return "", errors.New(errors.ErrUncoded, "database key format expected: `databases/orgID/dbID`")
	}

	return dax.DatabaseID(parts[2]), nil
}

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
		dax.NewQualifiedDatabaseID(
			dax.OrganizationID(parts[1]),
			dax.DatabaseID(parts[2]),
		),
		dax.TableID(parts[3]),
	), nil
}

// keyQualifedDatabaseID gets the QualifiedDatabaseID out of the key.
func keyQualifiedDatabaseID(key []byte) (dax.QualifiedDatabaseID, error) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 3 {
		return dax.QualifiedDatabaseID{}, errors.New(errors.ErrUncoded, "table key format expected: `databases/orgID/dbID`")
	}

	return dax.NewQualifiedDatabaseID(
		dax.OrganizationID(parts[1]),
		dax.DatabaseID(parts[2]),
	), nil
}

func timestamp() int64 {
	return time.Now().UnixNano()
}
