package dax

import (
	"crypto/rand"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/pql"
	uuid "github.com/satori/go.uuid"
)

////////////////////////////////////////////////////////////////////////////////
//
// Table
//
// The types defined below are used to standardize on tables and fields. Prior
// to introducing these types, the only way we could identify a table was by
// name, which wasn't even a defined type. Rather, we passed `string` values
// throughout the code.
//
// OrganizationID - carried over from ControlPlane; currently uuid
// DatabaseID - carried over from ControlPlane; currently uuid
// Database - base Database struct
// DatabaseKey - a string representation of OrganizationID and DatabaseID
// TableID - internally stored as a uint64; presented as a hex string.
// TableName - human-friendly string table name
// Table - base Table struct; includes a TableID and a TableName
// QualifiedDatabase - OrganizationID plus a Database
// QualifiedDatabaseID - combination of OrganizationID and DatabaseID
// QualifiedTable - QualifiedDatabaseID plus a Table
// QualifiedTableID - QualifiedDatabaseID plus a TableID
// TableKey - a string representation of OrganizationID, DatabaseID, and
//            TableID, which is safe to use as a FeatureBase index name.
//
// Example:
// OrganizationID - "29-ae44-41"
// DatabaseID - "75-d1a2-4f"
// TableID - 123456789 (hex string: "499602d2")
// TableName - foo
// Table - {ID:"499602d2", Name: "foo", Fields: ... }
// QualifierDatabaseID - {Org: "29-ae44-41", DB: "75-d1a2-4f"}
// QualifiedTable - {Org: "29-ae44-41", DB: "75-d1a2-4f", Table: *tbl}
// QualifiedTableID - {Org: "29-ae44-41", DB: "75-d1a2-4f", TableID: "499602d2"}
// TableKey - "tbl__29-ae44-41__75-d1a2-4f__499602d2"
//
////////////////////////////////////////////////////////////////////////////////

// TableKeyDelimiter is used to delimit the qualifier elements in the TableKey.
// While it might make more sense to use a pipe ("|") here, we instead use a
// double underscore because underscore is one of the few characters allowed by
// the FeatureBase index name restrictions, and we double it in a lame attempt
// to distinquish it from FeatureBase index names which contain a single
// underscore.
const TableKeyDelimiter = "__"

// PrefixDatabase is used as a prefix to DatabaseKey strings because FeatureBase
// indexes must start with an alpha (a-z) character. Because the string
// representation of a uuid (i.e. the OrganizationID value) can start with a
// numeric value, we can't have OrganizationId (or any of the other ID values
// which make up the DatabaseKey) be at the beginning of the DatabaseKey.
const PrefixDatabase = "db"

// PrefixTable is used as a prefix to TableKey strings because FeatureBase
// indexes must start with an alpha (a-z) character. Because the string
// representation of a uuid (i.e. the OrganizationID value) can start with a
// numeric value, we can't have OrganizationId (or any of the other ID values
// which make up the TableKey) be at the beginning of the TableKey.
const PrefixTable = "tbl"

// Base types.
const (
	BaseTypeBool      = "bool"      //
	BaseTypeDecimal   = "decimal"   //
	BaseTypeID        = "id"        // non-keyed mutex
	BaseTypeIDSet     = "idset"     // non-keyed set
	BaseTypeInt       = "int"       //
	BaseTypeString    = "string"    // keyed mutex
	BaseTypeStringSet = "stringset" // keyed set
	BaseTypeTimestamp = "timestamp" //

	DefaultPartitionN = 256

	PrimaryKeyFieldName = FieldName("_id")
)

// Schema contains a list of Tables.
type Schema struct {
	Tables []*Table
}

// Table returns the table with the provided name. If a table with that name
// does not exist, the returned boolean will be false.
func (s *Schema) Table(name TableName) (*Table, bool) {
	for _, tbl := range s.Tables {
		if tbl.Name == name {
			return tbl, true
		}
	}
	return nil, false
}

// OrganizationID is the unique organization identifier, currently generated by
// the Control Plane in a FeatureBase cloud implementation. In that
// implementation, its value is a uuid as a string, but there's nothing
// enforcing that; the value could be any string.
type OrganizationID string

// DatabaseID is the unique database identifier, currently generated by the
// Control Plane in a FeatureBase cloud implementation. In that implementation,
// its value is a uuid as a string, but there's nothing enforcing that; the
// value could be any string.
type DatabaseID string

// DatabaseIDs is a sortable slice of DatabaseID.
type DatabaseIDs []DatabaseID

func (s DatabaseIDs) Len() int           { return len(s) }
func (s DatabaseIDs) Less(i, j int) bool { return s[i] < s[j] }
func (s DatabaseIDs) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// DatabaseKey is a globally unique identifier for a database; it is effectively the
// compound key: (org, database). This is (hopefully) the value that will
// be used when interfacing with services which are unaware of qualifiers.
type DatabaseKey string

// QualifiedDatabaseID returns the QualifiedDatabaseID based on the key. If
// DatabaseKey can't be parsed into a valid (i.e. complete) QualifiedDatabaseID,
// then blank values are used where necessary.
func (dk DatabaseKey) QualifiedDatabaseID() QualifiedDatabaseID {
	qdbid, err := QualifiedDatabaseIDFromKey(string(dk))
	if err != nil {
		return NewQualifiedDatabaseID("", DatabaseID(dk))
	}
	return qdbid
}

// DatabaseName is a human-friendly string.
type DatabaseName string

// Database represents a database and its configuration.
type Database struct {
	ID      DatabaseID      `json:"id"`
	Name    DatabaseName    `json:"name"`
	Options DatabaseOptions `json:"options"`
	// Tables  []*Table        `json:"tables"`

	Description string `json:"description,omitempty"`
	Owner       string `json:"owner,omitempty"`
	CreatedAt   int64  `json:"createdAt,omitempty"`
	UpdatedAt   int64  `json:"updatedAt,omitempty"`
	UpdatedBy   string `json:"updatedBy,omitempty"`
}

// CreateID generates a unique identifier for Database. If Database has already
// been assigned an ID, then this no-ops. The reason for this is that the cloud
// implementation of FeatureBase may allocate an ID before calling
// CreateDatabase on the controller.
func (d *Database) CreateID() (DatabaseID, error) {
	if d.ID != "" {
		return d.ID, nil
	}

	id, err := uuid.NewV4()
	if err != nil {
		return "", errors.Wrap(err, "generating uuid")
	}

	d.ID = DatabaseID(id.String())

	return d.ID, nil
}

// DatabaseOptions are used to configure a database.
type DatabaseOptions struct {
	WorkersMin int `json:"workers-min"`
	WorkersMax int `json:"workers-max"`
}

// DatabaseOption is a string key representing a database option.
type DatabaseOption string

const (
	DatabaseOptionWorkersMin = "workers-min"
	DatabaseOptionWorkersMax = "workers-max"
)

// Set sets the specified option to the provided value.
func (opts *DatabaseOptions) Set(option string, value string) error {
	opt := strings.ToLower(option)
	switch opt {
	case DatabaseOptionWorkersMin:
		min, err := strconv.Atoi(value)
		if err != nil {
			return errors.Wrapf(err, "converting value to int: %s", value)
		}
		opts.WorkersMin = min
		// We don't currently expose WorkersMax because we aren't yet detecting
		// how to scale between a range, so for now we just keep it set to the
		// same value as WorkersMin.
		opts.WorkersMax = min
	default:
		return errors.Errorf("unsupported database option: %s", option)
	}

	return nil
}

// QualifiedDatabase is a Database along with its OrganizationID.
type QualifiedDatabase struct {
	OrganizationID OrganizationID `json:"org-id"`
	Database
}

// NewQualifiedDatabase returns the db as a QualifiedDatabase with the provided
// OrganizationID.
func NewQualifiedDatabase(orgID OrganizationID, db *Database) *QualifiedDatabase {
	return &QualifiedDatabase{
		OrganizationID: orgID,
		Database:       *db,
	}
}

type QualifiedDatabases []*QualifiedDatabase

// Key returns the string-encoded (delimited by DatabaseKeyDelimiter) globally
// unique DatabaseKey.
func (qdb QualifiedDatabase) Key() DatabaseKey {
	return qdb.QualifiedID().Key()
}

// String returns a human-friendly version of the QualifiedDatabase. It is only
// used for display purposes; it is not used as any kind of key.
func (qdb QualifiedDatabase) String() string {
	return fmt.Sprintf("%s (%s)", qdb.QualifiedID(), qdb.Name)
}

// QualifiedID returns the QualifiedDatabaseID for the database.
func (qdb *QualifiedDatabase) QualifiedID() QualifiedDatabaseID {
	return QualifiedDatabaseID{
		OrganizationID: qdb.OrganizationID,
		DatabaseID:     qdb.ID,
	}
}

// QualifiedDatabaseID is a DatabaseID along with its OrganizationID.
type QualifiedDatabaseID struct {
	OrganizationID OrganizationID `json:"org-id"`
	DatabaseID     DatabaseID     `json:"db-id"`
}

// NewQualifiedDatabaseID is a helper function used to create a
// QualifiedDatabaseID from the provided arguments.
func NewQualifiedDatabaseID(orgID OrganizationID, dbID DatabaseID) QualifiedDatabaseID {
	return QualifiedDatabaseID{
		OrganizationID: orgID,
		DatabaseID:     dbID,
	}
}

// String returns a human-friendly version of the QualifiedDatabaseID. It is only
// used for display purposes; it is not used as any kind of key. For that, see
// the QualifiedDatabaseID.Key() method and the DatabaseKey type.
func (qdbid QualifiedDatabaseID) String() string {
	return fmt.Sprintf("[%s:%s]", qdbid.OrganizationID, qdbid.DatabaseID)
}

// Key returns the string-encoded (delimited by TableKeyDelimiter) globally
// unique DatabaseKey. The key has a prefix because FeatureBase index name
// restrictions require the name to start with a non-numeric value, and since a
// uuid can contain a number as its first character, we have to prefix it with
// something.
func (qdbid QualifiedDatabaseID) Key() DatabaseKey {
	if qdbid.DatabaseID == "" {
		panic("QualifiedDatabaseID.Key called without an ID set")
	}
	return DatabaseKey(fmt.Sprintf("%s%s%s%s%s",
		PrefixDatabase,
		TableKeyDelimiter,
		qdbid.OrganizationID,
		TableKeyDelimiter,
		qdbid.DatabaseID))
}

// QualifiedDatabaseIDs is a list of QualifiedDatabaseID.
type QualifiedDatabaseIDs []QualifiedDatabaseID

func (s QualifiedDatabaseIDs) Len() int { return len(s) }
func (s QualifiedDatabaseIDs) Less(i, j int) bool {
	if s[i].OrganizationID != s[j].OrganizationID {
		return s[i].OrganizationID < s[j].OrganizationID
	}
	return s[i].DatabaseID < s[j].DatabaseID
}
func (s QualifiedDatabaseIDs) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// TableKeyer is an interface implemented by any type which can produce, and be
// represented by, a TableKey. In the case of a QualifiedTable, its TableKey
// might be something like `tbl__org__db__tableid`, while a general pilosa
// implemenation might represent a table as a basic table name `foo`.
type TableKeyer interface {
	Key() TableKey
}

// StringTableKeyer is a helper type which can wrap a string, making it a
// TableKeyer. This is useful for certain calls to Execute() which take a string
// index name.
type StringTableKeyer string

func (s StringTableKeyer) Key() TableKey {
	return TableKey(s)
}

// TableKey is a globally unique identifier for a table; it is effectively the
// compound key: (org, database, table). This is (hopefully) the value that will
// be used when interfacing with services which are unaware of table qualifiers.
// For example, the FeatureBase server has no notion of organization or
// database; its top level type is index/indexName/table. So in this case, until
// and unless we introduce table qualifiers into FeatureBase, we will use
// TableKey as the value for index.Name.
type TableKey string

func (tk TableKey) Key() TableKey { return tk }

// QualifiedTableID returns the QualifiedTableID based on the key. If TableKey
// can't be parsed into a valid (i.e. complete) QualifiedTableID, then blank
// values are used where necessary.
func (tk TableKey) QualifiedTableID() QualifiedTableID {
	qtid, err := QualifiedTableIDFromKey(string(tk))
	if err != nil {
		return NewQualifiedTableID(
			NewQualifiedDatabaseID("", ""),
			TableID(tk),
		)
	}
	return qtid
}

// TableKeys is a sortable slice of TableKey.
type TableKeys []TableKey

func (s TableKeys) Len() int           { return len(s) }
func (s TableKeys) Less(i, j int) bool { return s[i] < s[j] }
func (s TableKeys) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// TableID is a table identifier. It is unique within the scope of a
// QualifiedDatabaseID. Coupled with a QualifiedDatabaseID, it makes up a
// QualifiedTableID and, when encoded as a string, a TableKey.
type TableID string

// TableIDs is a sortable slice of TableID.
type TableIDs []TableID

func (s TableIDs) Len() int           { return len(s) }
func (s TableIDs) Less(i, j int) bool { return s[i] < s[j] }
func (s TableIDs) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// TableName is a human-friendly string. While it is not used as a primary key,
// uniqueness is generally enforced within the scope of a QualifiedDatabaseID.
type TableName string

// TableNames is a sortable slice of TableName.
type TableNames []TableName

func (s TableNames) Len() int           { return len(s) }
func (s TableNames) Less(i, j int) bool { return s[i] < s[j] }
func (s TableNames) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Table represents a table and its configuration.
type Table struct {
	ID         TableID   `json:"id,omitempty"`
	Name       TableName `json:"name,omitempty"`
	Fields     []*Field  `json:"fields"`
	PartitionN int       `json:"partitionN"`

	Description string `json:"description,omitempty"`
	Owner       string `json:"owner,omitempty"`
	CreatedAt   int64  `json:"createdAt,omitempty"`
	UpdatedAt   int64  `json:"updatedAt,omitempty"`
	UpdatedBy   string `json:"updatedBy,omitempty"`
}

func (t *Table) Key() TableKey {
	return TableKey(t.ID)
}

// CreateID generates a unique identifier for Table. If Table has already been
// assigned an ID, then an error is returned.
func (t *Table) CreateID() (TableID, error) {
	if t.ID != "" {
		return "", errors.Errorf("CreateID called on table %+v that already has ID", t)
	}

	// stub is prepended to the Table.ID as a way to make IDs somewhat
	// human-readable for debugging purposes. If the table name is changed after
	// its ID has been created, this could be confusing (because the stub
	// portion of the ID will still resemble the initial table name).
	//
	// In order to avoid creating an ID with a double underscore, we remove all
	// underscores from the original table name (because that's what we use in
	// TableKey as a delimiter). In addition to that, we remove any other
	// characters which are not valid as a pilosa index name.
	stub := regexp.MustCompile(`[^a-z0-9-]+`).ReplaceAllString(strings.ToLower(string(t.Name)), "")
	if len(stub) > 10 {
		stub = stub[:10]
	}

	rn := make([]byte, 8)
	if _, err := rand.Read(rn); err != nil {
		return "", errors.Wrap(err, "getting random data")
	}
	t.ID = TableID(fmt.Sprintf("%s_%x", stub, rn))

	return t.ID, nil
}

// NewTable returns a new instance of table with a pseudo-random ID which is
// assumed to be unique within the scope of a QualifiedDatabaseID.
func NewTable(name TableName) *Table {
	return &Table{
		Name:   name,
		Fields: make([]*Field, 0),
	}
}

// StringKeys returns true if the table's primary key is either a string or a
// concatenation of fields.
func (t *Table) StringKeys() bool {
	for _, fld := range t.Fields {
		if fld.IsPrimaryKey() {
			if fld.Type == BaseTypeString {
				return true
			}
			break
		}
	}
	return false
}

// HasValidPrimaryKey returns false if the table does not contain a primary key
// field (which is required), or if the primary key field is not a valid type.
func (t *Table) HasValidPrimaryKey() bool {
	for _, fld := range t.Fields {
		if !fld.IsPrimaryKey() {
			continue
		}

		if fld.Type == BaseTypeID || fld.Type == BaseTypeString {
			return true
		}
	}
	return false
}

// FieldNames returns the list of field names associated with the table.
func (t *Table) FieldNames() []FieldName {
	var ret []FieldName
	for _, f := range t.Fields {
		ret = append(ret, f.Name)
	}
	return ret
}

// Field returns the field with the provided name. If a field with that name
// does not exist, the returned boolean will be false.
func (t *Table) Field(name FieldName) (*Field, bool) {
	for _, fld := range t.Fields {
		if fld.Name == name {
			return fld, true
		}
	}
	return nil, false
}

// RemoveField removes the given field by name. It returns true if the field was
// removed.
func (t *Table) RemoveField(name FieldName) bool {
	for i, fld := range t.Fields {
		if fld.Name == name {
			t.Fields = append(t.Fields[:i], t.Fields[i+1:]...)
			return true
		}
	}
	return false
}

// CreateSQL returns the SQL CREATE TABLE string necessary to create the table.
func (t *Table) CreateSQL() string {
	sql := fmt.Sprintf("CREATE TABLE %s (", t.Name)

	cols := []string{}
	for _, fld := range t.Fields {
		cols = append(cols, fld.CreateSQL())
	}
	sql += strings.Join(cols, ", ")

	sql += fmt.Sprintf(") KEYPARTITIONS %d", t.PartitionN)

	return sql
}

// Tables is a sortable slice of Table.
type Tables []*Table

func (o Tables) Len() int           { return len(o) }
func (o Tables) Less(i, j int) bool { return o[i].Name < o[j].Name }
func (o Tables) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

////////////////////////////////////////////////

// QualifiedTableID is a globally unique table identifier. It is a
// sub-set of a QualifiedTable (i.e. it's just the identification
// portion). Most things will take a Name or an ID and do the right
// thing™.
type QualifiedTableID struct {
	QualifiedDatabaseID
	ID   TableID   `json:"id"`
	Name TableName `json:"name"`
}

// NewQualifiedTableID is a helper function used to create a QualifiedTableID
// from the provided arguments.
func NewQualifiedTableID(qdbid QualifiedDatabaseID, tid TableID) QualifiedTableID {
	return QualifiedTableID{
		QualifiedDatabaseID: qdbid,
		ID:                  tid,
	}
}

// QualifiedTableIDFromKey decodes a string key into a QualifiedTableID. The key
// is assumed to have been encoded using the QualifiedTableID.Key() method.
func QualifiedTableIDFromKey(key string) (QualifiedTableID, error) {
	parts := strings.Split(key, TableKeyDelimiter)
	switch len(parts) {
	case 4:
		// prefix|orgID|dbID|tblID
		return NewQualifiedTableID(
			NewQualifiedDatabaseID(
				OrganizationID(parts[1]),
				DatabaseID(parts[2]),
			),
			TableID(parts[3]),
		), nil
	default:
		return QualifiedTableID{}, errors.Errorf("invalid key: %s", key)
	}
}

// QualifiedDatabaseIDFromKey decodes a string key into a QualifiedDatabaseID.
// The key is assumed to have been encoded using the QualifiedDatabaseID.Key()
// method.
func QualifiedDatabaseIDFromKey(key string) (QualifiedDatabaseID, error) {
	parts := strings.Split(key, TableKeyDelimiter)
	switch len(parts) {
	case 3:
		// prefix|orgID|dbID
		return NewQualifiedDatabaseID(
			OrganizationID(parts[1]),
			DatabaseID(parts[2]),
		), nil
	default:
		return QualifiedDatabaseID{}, errors.Errorf("invalid key: %s", key)
	}
}

// String returns a human-friendly version of the QualifiedDatabaseID. It is
// only used for display purposes; it is not used as any kind of key. For that,
// see the QualifiedDatabaseID.Key() method.
func (qtid QualifiedTableID) String() string {
	if qtid.ID == "" {
		return fmt.Sprintf("%s%s", qtid.QualifiedDatabaseID, qtid.Name)
	}
	return fmt.Sprintf("%s%s", qtid.QualifiedDatabaseID, qtid.ID)
}

// Key returns the string-encoded (delimited by TableKeyDelimiter) globally
// unique TableKey. The key has a prefix because FeatureBase index name
// restrictions require the name to start with a non-numeric value, and since a
// uuid can contain a number as its first character, we have to prefix it with
// something.
func (qtid QualifiedTableID) Key() TableKey {
	if qtid.ID == "" {
		panic("QualifiedTableID.Key called without an ID set")
	}
	return TableKey(fmt.Sprintf("%s%s%s%s%s%s%s",
		PrefixTable,
		TableKeyDelimiter,
		qtid.OrganizationID,
		TableKeyDelimiter,
		qtid.DatabaseID,
		TableKeyDelimiter,
		qtid.ID))
}

// Equals returns true if `other` is the same as qtid. Note: the `Name` value is
// ignored in this comparison; only `QualifiedDatabaseID` and `ID` are
// considered.
func (qtid QualifiedTableID) Equals(other QualifiedTableID) bool {
	if qtid.QualifiedDatabaseID == other.QualifiedDatabaseID && qtid.ID == other.ID {
		return true
	}
	return false
}

// Qualifier returns the QualifiedDatabaseID (qdbid) portion of the
// QualifiedTableID (qtid).
func (qtid QualifiedTableID) Qualifier() QualifiedDatabaseID {
	return QualifiedDatabaseID{
		OrganizationID: qtid.OrganizationID,
		DatabaseID:     qtid.DatabaseID,
	}
}

////////////////////////////////////////////////

// QualifiedTable wraps Table and includes a QualifiedDatabaseID.
type QualifiedTable struct {
	QualifiedDatabaseID
	Table
}

// NewQualifiedTable returns the tbl as a QualifiedTable with the provided
// QualifiedDatabaseID.
func NewQualifiedTable(qdbid QualifiedDatabaseID, tbl *Table) *QualifiedTable {
	return &QualifiedTable{
		QualifiedDatabaseID: qdbid,
		Table:               *tbl,
	}
}

// Key returns the string-encoded (delimited by TableKeyDelimiter) globally
// unique TableKey.
func (qt QualifiedTable) Key() TableKey {
	return qt.QualifiedID().Key()
}

// String returns a human-friendly version of the QualifiedTable. It is only
// used for display purposes; it is not used as any kind of key.
func (qt QualifiedTable) String() string {
	return fmt.Sprintf("%s (%s)", qt.QualifiedID(), qt.Name)
}

// Qualifier returns the QualifiedDatabaseID portion of the QualifiedTable.
func (qt *QualifiedTable) Qualifier() QualifiedDatabaseID {
	return qt.QualifiedDatabaseID
}

// QualifiedID returns the QualifiedTableID for the table.
func (qt *QualifiedTable) QualifiedID() QualifiedTableID {
	return QualifiedTableID{
		QualifiedDatabaseID: qt.QualifiedDatabaseID,
		ID:                  qt.ID,
		Name:                qt.Name,
	}
}

// QualifiedTables is a sortable slice of QualifiedTable.
type QualifiedTables []*QualifiedTable

func (o QualifiedTables) Len() int           { return len(o) }
func (o QualifiedTables) Less(i, j int) bool { return o[i].ID < o[j].ID }
func (o QualifiedTables) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

// FieldName is a typed string used for field names.
type FieldName string

// BaseType is a typed string used for field types.
type BaseType string

// BaseTypeFromString converts a string to one of the defined BaseTypes. If the
// string does not match a BaseType, then an error is returned.
func BaseTypeFromString(s string) (BaseType, error) {
	lowered := strings.ToLower(s)
	switch lowered {
	case BaseTypeBool,
		BaseTypeDecimal,
		BaseTypeID,
		BaseTypeIDSet,
		BaseTypeInt,
		BaseTypeString,
		BaseTypeStringSet,
		BaseTypeTimestamp:
		return BaseType(lowered), nil
	default:
		return "", errors.Errorf("invalid field type: %s", s)
	}
}

// Field represents a field and its configuration.
type Field struct {
	Name    FieldName    `json:"name"`
	Type    BaseType     `json:"type"`
	Options FieldOptions `json:"options"`

	CreatedAt int64 `json:"createdAt,omitempty"`
}

// String returns the field name as a string.
func (f *Field) String() string {
	return string(f.Name)
}

// StringKeys returns true if the field uses string keys.
func (f *Field) StringKeys() bool {
	switch f.Type {
	case BaseTypeString, BaseTypeStringSet:
		return true
	}
	return false
}

// IsPrimaryKey returns true if the field is the primary key field (of either
// type ID or STRING).
func (f *Field) IsPrimaryKey() bool {
	return f.Name == PrimaryKeyFieldName
}

// CreateSQL returns the SQL representation of the field to be used in a CREATE
// TABLE statement.
func (f *Field) CreateSQL() string {
	sql := fmt.Sprintf("%s %s", f.Name, f.Type)

	// Apply constraints to all non-primarykey fields.
	if !f.IsPrimaryKey() {
		sql += f.constraints()
	}

	return sql
}

func (f *Field) constraints() string {
	sql := ""

	// Apply constraints.
	switch f.Type {
	case BaseTypeInt:
		sql += fmt.Sprintf(" MIN %d MAX %d", f.Options.Min.ToInt64(0), f.Options.Max.ToInt64(0))
	case BaseTypeID, BaseTypeString:
		if f.Options.CacheType != "" {
			sql += fmt.Sprintf(" CACHETYPE %s SIZE %d", f.Options.CacheType, f.Options.CacheSize)
		}
	case BaseTypeIDSet, BaseTypeStringSet:
		if f.Options.CacheType != "" {
			sql += fmt.Sprintf(" CACHETYPE %s SIZE %d", f.Options.CacheType, f.Options.CacheSize)
		}
		if f.Options.TimeQuantum != "" {
			sql += fmt.Sprintf(" TIMEQUANTUM '%s'", f.Options.TimeQuantum)
			if f.Options.TTL > 0 {
				sql += fmt.Sprintf(" TTL '%s'", f.Options.TTL)
			}
		}
	case BaseTypeTimestamp:
		if f.Options.TimeUnit != "" {
			sql += fmt.Sprintf(" TIMEUNIT '%s'", f.Options.TimeUnit)
			if !f.Options.Epoch.IsZero() {
				sql += fmt.Sprintf(" EPOCH '%s'", f.Options.Epoch.Format(time.RFC3339)) // time.RFC3339
			}
		}
	}

	return sql
}

// FieldOptions represents options to set when initializing a field.
type FieldOptions struct {
	Min            pql.Decimal   `json:"min,omitempty"`
	Max            pql.Decimal   `json:"max,omitempty"`
	Scale          int64         `json:"scale,omitempty"`
	NoStandardView bool          `json:"no-standard-view,omitempty"` // TODO: we should remove this
	CacheType      string        `json:"cache-type,omitempty"`
	CacheSize      uint32        `json:"cache-size,omitempty"`
	TimeUnit       string        `json:"time-unit,omitempty"`
	Epoch          time.Time     `json:"epoch,omitempty"`
	TimeQuantum    TimeQuantum   `json:"time-quantum,omitempty"`
	TTL            time.Duration `json:"ttl,omitempty"`
	ForeignIndex   string        `json:"foreign-index,omitempty"`
}
