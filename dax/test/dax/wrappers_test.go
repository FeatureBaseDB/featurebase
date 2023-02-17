package dax_test

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/computer"
	controllerclient "github.com/featurebasedb/featurebase/v3/dax/controller/client"
)

var _ computer.Registrar = (*wrappedControllerClient)(nil)

//var _ dax.Schemar = (*wrappedControllerClient)(nil)

// wrappedControllerClient is a wrapper around the controller client which we
// use in tests to ensure that all of the methods for the computer.Registrar
// interface are covered by these tests. The idea being that if someone modifes
// the interface to include a new `Foo()` method, this test will no longer
// compile, and the developer will be directected here to add the appropriate
// tests. There's probably a more automated way to do this wil the `reflect`
// package, but that seems overly compilicated right now.
type wrappedControllerClient struct {
	cli *controllerclient.Client
	dax.Schemar
}

func newWrappedControllerClient(cli *controllerclient.Client) *wrappedControllerClient {
	return &wrappedControllerClient{
		cli:     cli,
		Schemar: dax.NewNopSchemar(),
	}
}

func (w *wrappedControllerClient) RegisterNode(ctx context.Context, node *dax.Node) error {
	return w.cli.RegisterNode(ctx, node)
}

func (w *wrappedControllerClient) CheckInNode(ctx context.Context, node *dax.Node) error {
	return w.cli.CheckInNode(ctx, node)
}

// Schemar

/*
func (w *wrappedControllerClient) 	CreateDatabase(context.Context, *QualifiedDatabase) error
func (w *wrappedControllerClient) 	DropDatabase(context.Context, QualifiedDatabaseID) error

	DatabaseByName(ctx context.Context, orgID OrganizationID, dbname DatabaseName) (*QualifiedDatabase, error)
	DatabaseByID(ctx context.Context, qdbid QualifiedDatabaseID) (*QualifiedDatabase, error)

	SetDatabaseOption(ctx context.Context, qdbid QualifiedDatabaseID, option string, value string) error

	Databases(context.Context, OrganizationID, ...DatabaseID) ([]*QualifiedDatabase, error)

	CreateTable(ctx context.Context, qtbl *QualifiedTable) error
	DropTable(ctx context.Context, qtid QualifiedTableID) error

	TableByName(ctx context.Context, qdbid QualifiedDatabaseID, tname TableName) (*QualifiedTable, error)
	TableByID(ctx context.Context, qtid QualifiedTableID) (*QualifiedTable, error)

	Tables(ctx context.Context, qdbid QualifiedDatabaseID, tids ...TableID) ([]*QualifiedTable, error)

	CreateField(ctx context.Context, qtid QualifiedTableID, fld *Field) error
	DropField(ctx context.Context, qtid QualifiedTableID, fname FieldName) error
*/
