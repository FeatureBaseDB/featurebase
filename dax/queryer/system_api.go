package queryer

import (
	featurebase "github.com/featurebasedb/featurebase/v3"
)

// systemAPI is a no-op implementation of the systemAPI.
type systemAPI struct {
	featurebase.NopSystemAPI
}

func newSystemAPI() *systemAPI {
	return &systemAPI{}
}

func (s *systemAPI) ClusterName() string {
	return featurebase.FlavorServerless
}
