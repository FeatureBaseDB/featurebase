package balancer_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/require"
)

func TestWorkerServiceAndProvider(t *testing.T) {
	tx, err := SQLTransactor.BeginTx(context.Background(), true)
	require.NoError(t, err, "getting transaction")

	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("rolling back: %v", err)
		}
	}()

	balancer := sqldb.NewBalancer(logger.StderrLogger)

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
	t.Run("create and query a WSP", func(t *testing.T) {
		err = balancer.CreateWorkerServiceProvider(tx, wsp)
		require.NoError(t, err, "creating initial wsp")

		wsps, err := balancer.WorkerServiceProviders(tx)
		require.NoError(t, err, "reading wsps")
		require.Equal(t, dax.WorkerServiceProviders{wsp}, wsps)

	})

	t.Run("create another wsp and query both", func(t *testing.T) {
		err = balancer.CreateWorkerServiceProvider(tx, wsp2)
		require.NoError(t, err, "creating wsp2")

		wsps, err := balancer.WorkerServiceProviders(tx)
		require.NoError(t, err, "reading wsps")
		require.ElementsMatch(t, dax.WorkerServiceProviders{wsp, wsp2}, wsps)
	})

	t.Run("create 3 worker services across 2 wsps, and query", func(t *testing.T) {
		ws := dax.WorkerService{
			ID:                      "wsID1",
			Roles:                   []dax.RoleType{"compute"},
			WorkerServiceProviderID: "wspID1",
			DatabaseID:              "",
			WorkersMin:              1,
			WorkersMax:              1,
		}
		err = balancer.CreateWorkerService(tx, ws)
		require.NoError(t, err, "creating worker service 1")

		ws2 := dax.WorkerService{
			ID:                      "wsID2",
			Roles:                   []dax.RoleType{"compute"},
			WorkerServiceProviderID: "wspID1",
			DatabaseID:              "",
			WorkersMin:              1,
			WorkersMax:              1,
		}
		err = balancer.CreateWorkerService(tx, ws2)
		require.NoError(t, err, "creating worker service 2")

		ws3 := dax.WorkerService{
			ID:                      "wsID3",
			Roles:                   []dax.RoleType{"compute", "translate"},
			WorkerServiceProviderID: "wspID2",
			DatabaseID:              "",
			WorkersMin:              1,
			WorkersMax:              1,
		}
		err = balancer.CreateWorkerService(tx, ws3)
		require.NoError(t, err, "creating worker service 3")

		wsvcs, err := balancer.WorkerServices(tx, "")
		require.NoError(t, err, "getting all worker services")
		require.ElementsMatch(t, dax.WorkerServices{ws, ws2, ws3}, wsvcs)

		wsvcs, err = balancer.WorkerServices(tx, "wspID1")
		require.NoError(t, err, "getting all worker services")
		require.ElementsMatch(t, dax.WorkerServices{ws, ws2}, wsvcs)
	})

}
