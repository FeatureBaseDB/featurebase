package controller

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/dax"
)

type WorkerServiceProvider interface {
	ClaimService(ctx context.Context, svc *dax.WorkerService) error
	UpdateService(ctx context.Context, svc *dax.WorkerService) error
	DropService(ctx context.Context, svc *dax.WorkerService) error
}
