package controller

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/dax"
)

type Director interface {
	SendDirective(ctx context.Context, dir *dax.Directive) error
	SendSnapshotShardDataRequest(ctx context.Context, req *dax.SnapshotShardDataRequest) error
	SendSnapshotTableKeysRequest(ctx context.Context, req *dax.SnapshotTableKeysRequest) error
	SendSnapshotFieldKeysRequest(ctx context.Context, req *dax.SnapshotFieldKeysRequest) error
}

// Ensure type implements interface.
var _ Director = &NopDirector{}

// NopDirector is a no-op implementation of the Director interface.
type NopDirector struct{}

func NewNopDirector() *NopDirector {
	return &NopDirector{}
}

func (d *NopDirector) SendDirective(ctx context.Context, dir *dax.Directive) error {
	return nil
}

func (d *NopDirector) SendSnapshotShardDataRequest(ctx context.Context, req *dax.SnapshotShardDataRequest) error {
	return nil
}

func (d *NopDirector) SendSnapshotTableKeysRequest(ctx context.Context, req *dax.SnapshotTableKeysRequest) error {
	return nil
}

func (d *NopDirector) SendSnapshotFieldKeysRequest(ctx context.Context, req *dax.SnapshotFieldKeysRequest) error {
	return nil
}
