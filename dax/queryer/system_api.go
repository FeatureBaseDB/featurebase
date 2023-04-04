package queryer

import (
	"context"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
)

// systemAPI is an implementation of the systemAPI.
type systemAPI struct {
	featurebase.NopSystemAPI
	controller dax.Controller
	qdbid      dax.QualifiedDatabaseID
}

func newSystemAPI(c dax.Controller, qdbid dax.QualifiedDatabaseID) *systemAPI {
	return &systemAPI{
		controller: c,
		qdbid:      qdbid,
	}
}

// ClusterNodes returns a list of featurebase.ClusterNodes
// with length of the minimum number of workers
func (s *systemAPI) ClusterNodes() []featurebase.ClusterNode {

	ctx := context.Background()

	qdb, err := s.controller.DatabaseByID(ctx, s.qdbid)
	if err != nil {
		return []featurebase.ClusterNode{}
	}
	out := make([]featurebase.ClusterNode, qdb.Options.WorkersMin)
	return out
}

func (s *systemAPI) PlatformDescription() string {
	return "Serverless"
}

func (s *systemAPI) ClusterName() string {
	return "Serverless"
}

func (s *systemAPI) ClusterNodeCount() int {
	ctx := context.Background()

	qdb, err := s.controller.DatabaseByID(ctx, s.qdbid)
	if err != nil {
		return -1
	}
	return qdb.Options.WorkersMin
}

func (s *systemAPI) ClusterState() string {
	return "NORMAL"
}
