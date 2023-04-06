package sqldb

import (
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/gobuffalo/nulls"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
)

func TestJobsNotUpdated(t *testing.T) {
	u, _ := uuid.NewV4()
	u2, _ := uuid.NewV4()
	incJobs := []dax.Job{"job1", "job2", "job3", "job4"}
	created := models.Jobs{
		models.Job{
			ID:   u,
			Name: "job2",
		},
	}

	toCreate := jobsNotAssigned(incJobs, created, dax.RoleTypeCompute, &models.Worker{
		ID:          u2,
		RoleCompute: true,
		DatabaseID:  nulls.NewString("dbid"),
	})

	require.Equal(t, 3, len(toCreate))

	// Test when 0 jobs are updated
	toCreate = jobsNotAssigned(incJobs, models.Jobs{}, dax.RoleTypeCompute, &models.Worker{
		ID:          u2,
		RoleCompute: true,
		DatabaseID:  nulls.NewString("dbid"),
	})

	require.Equal(t, 4, len(toCreate))
}
