package systemlayer

import pilosa "github.com/featurebasedb/featurebase/v3"

// SystemLayer is a struct to hold internal FeatureBase state
// Initially this is just the execution requests, but later may include other
// internal state (Buffer Pool?)
type SystemLayer struct {
	executionRequests pilosa.ExecutionRequestsAPI
}

func NewSystemLayer() *SystemLayer {
	return &SystemLayer{
		executionRequests: NewExecutionRequestsAPI(),
	}
}

func (e *SystemLayer) ExecutionRequests() pilosa.ExecutionRequestsAPI {
	return e.executionRequests
}
