package dax_test

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/computer"
	controllerclient "github.com/featurebasedb/featurebase/v3/dax/controller/client"
)

var _ computer.Registrar = (*wrappedControllerClient)(nil)
var _ dax.Schemar = (*wrappedControllerClient)(nil)

// wrappedControllerClient is a wrapper around the controller client which we
// use in tests to ensure that all of the methods for the computer.Registrar
// interface are covered by these tests. The idea being that if someone modifes
// the interface to include a new `Foo()` method, this test will no longer
// compile, and the developer will be directected here to add the appropriate
// tests. There's probably a more automated way to do this wil the `reflect`
// package, but that seems overly compilicated right now.
type wrappedControllerClient struct {
	cli *controllerclient.Client
}

func newWrappedControllerClient(cli *controllerclient.Client) *wrappedControllerClient {
	return &wrappedControllerClient{
		cli: cli,
	}
}

// Registrar

func (w *wrappedControllerClient) RegisterNode(ctx context.Context, node *dax.Node) error {
	return w.cli.RegisterNode(ctx, node)
}

func (w *wrappedControllerClient) CheckInNode(ctx context.Context, node *dax.Node) error {
	return w.cli.CheckInNode(ctx, node)
}

// Schemar

func (w *wrappedControllerClient) CreateDatabase(ctx context.Context, qdb *dax.QualifiedDatabase) error {
	return w.cli.CreateDatabase(ctx, qdb)
}

func (w *wrappedControllerClient) DropDatabase(ctx context.Context, qdbid dax.QualifiedDatabaseID) error {
	return w.cli.DropDatabase(ctx, qdbid)
}

func (w *wrappedControllerClient) DatabaseByName(ctx context.Context, orgID dax.OrganizationID, dbname dax.DatabaseName) (*dax.QualifiedDatabase, error) {
	return w.cli.DatabaseByName(ctx, orgID, dbname)
}

func (w *wrappedControllerClient) DatabaseByID(ctx context.Context, qdbid dax.QualifiedDatabaseID) (*dax.QualifiedDatabase, error) {
	return w.cli.DatabaseByID(ctx, qdbid)
}

func (w *wrappedControllerClient) SetDatabaseOption(ctx context.Context, qdbid dax.QualifiedDatabaseID, option string, value string) error {
	return w.cli.SetDatabaseOption(ctx, qdbid, option, value)
}

func (w *wrappedControllerClient) Databases(ctx context.Context, orgID dax.OrganizationID, dbIDs ...dax.DatabaseID) ([]*dax.QualifiedDatabase, error) {
	return w.cli.Databases(ctx, orgID, dbIDs...)
}

func (w *wrappedControllerClient) CreateTable(ctx context.Context, qtbl *dax.QualifiedTable) error {
	return w.cli.CreateTable(ctx, qtbl)
}

func (w *wrappedControllerClient) DropTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	return w.cli.DropTable(ctx, qtid)
}

func (w *wrappedControllerClient) TableByName(ctx context.Context, qdbid dax.QualifiedDatabaseID, tname dax.TableName) (*dax.QualifiedTable, error) {
	return w.cli.TableByName(ctx, qdbid, tname)
}

func (w *wrappedControllerClient) TableByID(ctx context.Context, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	return w.cli.TableByID(ctx, qtid)
}

func (w *wrappedControllerClient) Tables(ctx context.Context, qdbid dax.QualifiedDatabaseID, tids ...dax.TableID) ([]*dax.QualifiedTable, error) {
	return w.cli.Tables(ctx, qdbid, tids...)
}

func (w *wrappedControllerClient) CreateField(ctx context.Context, qtid dax.QualifiedTableID, fld *dax.Field) error {
	return w.cli.CreateField(ctx, qtid, fld)
}

func (w *wrappedControllerClient) DropField(ctx context.Context, qtid dax.QualifiedTableID, fname dax.FieldName) error {
	return w.cli.DropField(ctx, qtid, fname)
}
