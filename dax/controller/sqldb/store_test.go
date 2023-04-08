package sqldb_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	tx, err := SQLTransactor.BeginTx(context.Background(), true)
	require.NoError(t, err, "getting transaction")

	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("rolling back: %v", err)
		}
	}()

	var s controller.Store = sqldb.NewStore(nil)

	wsp := dax.WorkerServiceProvider{
		ID:          "wspID1",
		Roles:       []dax.RoleType{"compute"},
		Address:     "wsp1.example.com:8082",
		Description: "the description",
	}
	wsp2 := dax.WorkerServiceProvider{
		ID:          "wspID2",
		Roles:       []dax.RoleType{"compute", "translate"},
		Address:     "wsp2.example.com:8082",
		Description: "the description2",
	}

	t.Run("add a worker service provider", func(t *testing.T) {
		err := s.CreateWorkerServiceProvider(tx, wsp)
		require.NoError(t, err)
	})

	t.Run("read the wsp back", func(t *testing.T) {
		wsps, err := s.WorkerServiceProviders(tx)
		require.NoError(t, err)
		require.Equal(t, dax.WorkerServiceProviders{wsp}, wsps)
	})

	t.Run("add another worker service provider", func(t *testing.T) {
		err := s.CreateWorkerServiceProvider(tx, wsp2)
		require.NoError(t, err)
	})

	t.Run("read both wsps back", func(t *testing.T) {
		wsps, err := s.WorkerServiceProviders(tx)
		require.NoError(t, err)
		require.ElementsMatch(t, dax.WorkerServiceProviders{wsp, wsp2}, wsps)
	})

	// Three worker services, two from WSP1, 3rd from WSP2
	ws := dax.WorkerService{
		ID:                      "wsID1",
		Roles:                   []dax.RoleType{"compute"},
		WorkerServiceProviderID: "wspID1",
		DatabaseID:              "",
		WorkersMin:              1,
		WorkersMax:              1,
	}

	ws2 := ws
	ws2.ID = "wsID2"

	ws3 := ws
	ws3.ID = "wsID3"
	ws3.Roles = []dax.RoleType{"compute", "translate"}
	ws3.WorkerServiceProviderID = "wspID2"

	t.Run("create 3 worker services across 2 wsps", func(t *testing.T) {
		require.NoError(t, s.CreateWorkerService(tx, ws), "creating worker service 1")
		require.NoError(t, s.CreateWorkerService(tx, ws2), "creating worker service 2")
		require.NoError(t, s.CreateWorkerService(tx, ws3), "creating worker service 3")
	})

	t.Run("query all worker services regardless of wsp", func(t *testing.T) {
		wsvcs, err := s.WorkerServices(tx, "")
		require.NoError(t, err, "getting all worker services")
		require.ElementsMatch(t, dax.WorkerServices{ws, ws2, ws3}, wsvcs)
	})

	t.Run("query worker services for wspID1", func(t *testing.T) {
		wsvcs, err := s.WorkerServices(tx, "wspID1")
		require.NoError(t, err, "getting all worker services")
		require.ElementsMatch(t, dax.WorkerServices{ws, ws2}, wsvcs)
	})

	node1 := &dax.Node{
		Address:   "nodeaddr1",
		ServiceID: ws.ID,
		RoleTypes: ws.Roles,
	}

	t.Run("register a worker in ws1", func(t *testing.T) {
		qdbidp, err := s.AddWorker(tx, node1)
		require.NoError(t, err)
		require.Nil(t, qdbidp)
	})

	t.Run("list workers", func(t *testing.T) {
		addrs, err := s.ListWorkers(tx, dax.RoleTypeCompute, ws.ID)
		require.NoError(t, err)
		require.Equal(t, dax.Addresses{node1.Address}, addrs)
	})

	t.Run("worker for address", func(t *testing.T) {
		node, err := s.WorkerForAddress(tx, "nodeaddr1")
		var expectedDBID *dax.DatabaseID = nil
		require.NoError(t, err)
		require.Equal(t, dax.Address("nodeaddr1"), node.Address)
		require.Equal(t, ws.ID, node.ServiceID)
		require.Equal(t, expectedDBID, node.DatabaseID)
	})
}
