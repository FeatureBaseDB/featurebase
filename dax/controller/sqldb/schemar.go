package sqldb

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/schemar"
	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/featurebasedb/featurebase/v3/logger"
)

func NewSchemar(log logger.Logger) schemar.Schemar {
	if log == nil {
		log = logger.NopLogger
	}
	return &Schemar{
		log: log,
	}
}

// TODO rename to "schemar" once we get rid of that pesky schemar package
type Schemar struct {
	log logger.Logger
}

func (s *Schemar) CreateDatabase(tx dax.Transaction, qdb *dax.QualifiedDatabase) error {
	// Ensure the database id is not blank.
	if qdb.ID == "" {
		return schemar.NewErrDatabaseIDInvalid(qdb.ID)
	}

	// Ensure the database name is not blank.
	if qdb.Name == "" {
		return schemar.NewErrDatabaseNameInvalid(qdb.Name)
	}

	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	if exists, err := dt.C.Where("id = ?", qdb.Database.ID).Exists(&models.Database{}); err != nil {
		return errors.Wrap(err, "checking database existence")
	} else if exists {
		return dax.NewErrDatabaseIDExists(qdb.QualifiedID())
	}

	org := &models.Organization{ID: string(qdb.OrganizationID)}
	if ok, err := dt.C.Where("id = ?", qdb.OrganizationID).Exists(org); err != nil {
		return errors.Wrap(err, "checking for org")
	} else if !ok {
		if err := dt.C.Create(org); err != nil {
			return errors.Wrap(err, "creating organization")
		}
	}

	db := toModelDatabase(qdb)

	if err := dt.C.Create(db); err != nil {
		return errors.Wrap(err, "creating database object")
	}

	return nil
}

func (s *Schemar) DropDatabase(tx dax.Transaction, qdb dax.QualifiedDatabaseID) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	db := &models.Database{}
	err := dt.C.RawQuery("DELETE from databases where id = ? RETURNING id", qdb.DatabaseID).First(db)
	if isNoRowsError(err) {
		return dax.NewErrDatabaseIDDoesNotExist(qdb)
	}

	return errors.Wrap(err, "deleting database")
}

func (s *Schemar) DatabaseByName(tx dax.Transaction, orgID dax.OrganizationID, dbname dax.DatabaseName) (*dax.QualifiedDatabase, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	db := &models.Database{}
	err := dt.C.Where("organization_id = ? and name = ?", orgID, dbname).First(db)
	if isNoRowsError(err) {
		return nil, dax.NewErrDatabaseNameDoesNotExist(dbname)
	} else if err != nil {
		return nil, errors.Wrap(err, "finding database")
	}

	return toQualifiedDatabase(db), nil
}

func toModelDatabase(qdb *dax.QualifiedDatabase) *models.Database {
	db := qdb.Database
	return &models.Database{
		ID:          string(db.ID),
		Name:        db.Name,
		WorkersMin:  db.Options.WorkersMin,
		WorkersMax:  db.Options.WorkersMax,
		Description: db.Description,
		Owner:       db.Owner,
		UpdatedBy:   db.UpdatedBy,
		//		Tables:         []*models.Table{},
		OrganizationID: string(qdb.OrganizationID),
		// CreatedAt:      time.Unix(db.CreatedAt, 0),
		// UpdatedAt:      time.Unix(db.UpdatedAt),
	}
}

func toQualifiedDatabase(db *models.Database) *dax.QualifiedDatabase {
	return &dax.QualifiedDatabase{
		OrganizationID: dax.OrganizationID(db.OrganizationID),
		Database: dax.Database{
			ID:   dax.DatabaseID(db.ID),
			Name: dax.DatabaseName(db.Name),
			Options: dax.DatabaseOptions{
				WorkersMin: db.WorkersMin,
				WorkersMax: db.WorkersMax,
			},
			Description: db.Description,
			Owner:       db.Owner,
			CreatedAt:   db.CreatedAt.Unix(), // TODO is this right, or UnixNano, or...?
			UpdatedAt:   db.UpdatedAt.Unix(),
			UpdatedBy:   db.UpdatedBy,
		}}
}

func (s *Schemar) DatabaseByID(tx dax.Transaction, qdb dax.QualifiedDatabaseID) (*dax.QualifiedDatabase, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	db := &models.Database{}
	err := dt.C.Find(db, string(qdb.DatabaseID))
	if isNoRowsError(err) {
		return nil, dax.NewErrDatabaseIDDoesNotExist(qdb)
	} else if err != nil {
		return nil, errors.Wrap(err, "finding DB")
	}
	return toQualifiedDatabase(db), nil
}

func (s *Schemar) SetDatabaseOption(tx dax.Transaction, qdbid dax.QualifiedDatabaseID, option string, value string) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	var val int64
	var err error
	switch option {
	case dax.DatabaseOptionWorkersMin:
		option = "workers_min" // convert to table column name
		val, err = strconv.ParseInt(value, 0, 64)
		if err != nil {
			return errors.Wrap(err, "parsing workers min value")
		}
	case dax.DatabaseOptionWorkersMax:
		val, err = strconv.ParseInt(value, 0, 64)
		option = "workers_max" // convert to table column name
		if err != nil {
			return errors.Wrap(err, "parsing workers max value")
		}
	default:
		return errors.Errorf("unsupported database option: %s", option)
	}
	db := &models.Database{}
	err = dt.C.RawQuery(fmt.Sprintf("UPDATE databases set %s = ? WHERE id = ? RETURNING id", option), val, qdbid.DatabaseID).First(db)
	if isNoRowsError(err) {
		return dax.NewErrDatabaseIDDoesNotExist(qdbid)
	} else if err != nil {
		return errors.Wrap(err, "updating option")
	}

	return errors.Wrap(err, "updating database option")
}

// Databases returns a list of databases. If the list of DatabaseIDs is
// empty, all databases will be returned. If greater than zero DatabaseIDs
// are passed in the second argument, only databases matching those IDs will
// be returned.
func (s *Schemar) Databases(tx dax.Transaction, orgID dax.OrganizationID, dbIDs ...dax.DatabaseID) ([]*dax.QualifiedDatabase, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}
	s.log.Debugf("Schemar: Databases: orgID: %s dbIDs: %v", orgID, dbIDs)

	dbs := []*models.Database{}
	q := dt.C.Q()
	if orgID != "" {
		q = q.Where("organization_id = ?", orgID)
	}
	if len(dbIDs) > 0 {
		ifaceIDs := make([]interface{}, len(dbIDs))
		for i, dbID := range ifaceIDs {
			ifaceIDs[i] = dbID
		}
		q = q.Where("id in (?)", ifaceIDs...)
	}
	err := q.Order("created_at asc").All(&dbs)
	if err != nil {
		return nil, errors.Wrap(err, "finding databases")
	}

	ret := make([]*dax.QualifiedDatabase, len(dbs))
	for i, db := range dbs {
		ret[i] = toQualifiedDatabase(db)
	}
	s.log.Debugf("Schemar: Databases: returning %+v", ret)

	return ret, nil
}

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

	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	tbl := toModelTable(qtbl)

	err := dt.C.Eager().Create(tbl)
	if isViolatesUniqueConstraint(err) {
		return dax.NewErrTableIDExists(qtbl.QualifiedID())
	}

	return errors.Wrap(err, "creating database object")
}

func toModelTable(qtbl *dax.QualifiedTable) *models.Table {
	columns := make([]models.Column, len(qtbl.Fields))
	for i, fld := range qtbl.Fields {
		columns[i] = toModelColumn(qtbl.Key(), fld)
	}
	return &models.Table{
		ID:             string(qtbl.Key()),
		Name:           qtbl.Name,
		Owner:          qtbl.Owner,
		OrganizationID: qtbl.OrganizationID,
		Columns:        columns,
		UpdatedBy:      qtbl.UpdatedBy,
		DatabaseID:     string(qtbl.QualifiedDatabaseID.DatabaseID),
		Description:    qtbl.Description,
		PartitionN:     qtbl.PartitionN,
	}
}

func toModelColumn(tk dax.TableKey, fld *dax.Field) models.Column {
	optBytes, err := json.Marshal(fld.Options)
	if err != nil {
		panic(err)
	}
	return models.Column{
		Name:        fld.Name,
		Type:        fld.Type,
		TableID:     string(tk),
		Constraints: "TODO: unimplemented",
		Options:     string(optBytes),
	}
}

func toField(col models.Column) *dax.Field {
	opts := dax.FieldOptions{}
	err := json.Unmarshal([]byte(col.Options), &opts)
	if err != nil {
		panic(err)
	}
	return &dax.Field{
		Name:      col.Name,
		Type:      col.Type,
		Options:   opts,
		CreatedAt: col.CreatedAt.Unix(),
	}
}

func toQualifiedTable(mtbl *models.Table) *dax.QualifiedTable {
	fields := make([]*dax.Field, len(mtbl.Columns))
	for i, col := range mtbl.Columns {
		fields[i] = toField(col)
	}
	return &dax.QualifiedTable{
		QualifiedDatabaseID: dax.QualifiedDatabaseID{
			OrganizationID: mtbl.OrganizationID,
			DatabaseID:     dax.DatabaseID(mtbl.DatabaseID),
		},
		Table: dax.Table{
			ID:          dax.TableKey(mtbl.ID).QualifiedTableID().ID,
			Name:        mtbl.Name,
			Fields:      fields,
			PartitionN:  mtbl.PartitionN,
			Description: mtbl.Description,
			Owner:       mtbl.Owner,
			CreatedAt:   mtbl.CreatedAt.Unix(),
			UpdatedAt:   mtbl.UpdatedAt.Unix(),
			UpdatedBy:   mtbl.UpdatedBy,
		},
	}
}

func (s *Schemar) DropTable(tx dax.Transaction, qtid dax.QualifiedTableID) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	err := dt.C.Destroy(&models.Table{ID: string(qtid.Key())})
	return errors.Wrap(err, "destroying table")
}

func (s *Schemar) CreateField(tx dax.Transaction, qtid dax.QualifiedTableID, field *dax.Field) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}
	if field.Name == "" {
		return schemar.NewErrFieldNameInvalid(field.Name)
	}

	// we could probably make this a single query with an INSERT WHERE
	// (subselect), but then would have to construct the whole insert
	// by hand which would be annoying and error prone to keep up to
	// date
	cols := &models.Columns{}
	err := dt.C.Where("name = ? and table_id = ?", field.Name, qtid.Key()).All(cols)
	if err != nil {
		return errors.Wrap(err, "looking up field")
	}
	if len(*cols) > 0 {
		return dax.NewErrFieldExists(field.Name)
	}

	col := toModelColumn(qtid.Key(), field)
	err = dt.C.Create(&col)
	return errors.Wrap(err, "creating column")
}

func (s *Schemar) DropField(tx dax.Transaction, qtid dax.QualifiedTableID, fieldName dax.FieldName) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	col := &models.Column{}
	err := dt.C.Where("table_id = ? and name = ?", qtid.Key(), fieldName).First(col)
	// TODO if simply not found
	// return dax.NewErrFieldDoesNotExist(fldName)
	if err != nil {
		if isNoRowsError(err) {
			return dax.NewErrFieldDoesNotExist(fieldName)
		}

		return errors.Wrap(err, "querying for field")
	}

	err = dt.C.Destroy(col)

	return errors.Wrap(err, "destroying col")
}

func (s *Schemar) Table(tx dax.Transaction, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	tbl := &models.Table{}
	if qtid.ID != "" {
		if err := dt.C.Eager().Find(tbl, qtid.Key()); err != nil {
			if isNoRowsError(err) {
				return nil, dax.NewErrTableIDDoesNotExist(qtid)
			}
			return nil, errors.Wrap(err, "finding table by ID")
		}
	} else {
		if err := dt.C.Eager().Where("database_id = ? and name = ?", qtid.DatabaseID, qtid.Name).First(tbl); err != nil {
			if isNoRowsError(err) {
				return nil, dax.NewErrTableNameDoesNotExist(qtid.Name)
			}
			return nil, errors.Wrap(err, "finding table by name")
		}
	}

	return toQualifiedTable(tbl), nil
}

// Tables returns a list of tables in the given database. If tableIDs
// are given, only tables with matching IDs are returned.
func (s *Schemar) Tables(tx dax.Transaction, qdbid dax.QualifiedDatabaseID, tableIDs ...dax.TableID) ([]*dax.QualifiedTable, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	query := dt.C.Where("database_id = ?", qdbid.DatabaseID)
	if len(tableIDs) > 0 {
		ifaceIDs := make([]interface{}, len(tableIDs))
		for i, tableID := range tableIDs {
			ifaceIDs[i] = dax.QualifiedTableID{QualifiedDatabaseID: qdbid, ID: tableID}.Key()
		}
		query = query.Where("id in (?)", ifaceIDs)
	}
	tables := []*models.Table{}
	err := query.Eager().Order("created_at asc").All(&tables)
	if err != nil {
		return nil, errors.Wrap(err, "querying all tables")
	}

	ret := make([]*dax.QualifiedTable, len(tables))
	for i, tab := range tables {
		ret[i] = toQualifiedTable(tab)
	}

	return ret, nil
}

// TableID is a reverse-lookup method to get the TableID for a given
// qualified TableName.
func (s *Schemar) TableID(tx dax.Transaction, qdbid dax.QualifiedDatabaseID, tableName dax.TableName) (dax.QualifiedTableID, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.QualifiedTableID{}, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	tbl := &models.Table{}
	if err := dt.C.Where("database_id = ? and name = ?", qdbid.DatabaseID, tableName).First(tbl); err != nil {
		if isNoRowsError(err) {
			return dax.QualifiedTableID{}, dax.NewErrTableNameDoesNotExist(tableName)
		}
		return dax.QualifiedTableID{}, errors.Wrapf(err, "looking up table by name '%s', dbid: '%s'", tableName, qdbid.DatabaseID)
	}

	return dax.TableKey(tbl.ID).QualifiedTableID(), nil
}
