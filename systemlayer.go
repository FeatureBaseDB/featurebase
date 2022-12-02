package pilosa

import (
	"strings"
	"time"
)

// ExecutionRequest holds data about an (sql) execution request
type ExecutionRequest struct {
	// the id of the request
	RequestID string
	// the id of the user
	UserID string
	// time the request started
	StartTime time.Time
	// time the request finished - zero iif it has not finished
	EndTime time.Time
	// status of the request 'running' or 'complete' now, could have other values later
	Status string
	// future: if the request is waiting, the type of wait that is occuring
	WaitType string
	// future: the cumulative wait time for this request
	WaitTime time.Duration
	// futuure: if the request is waiting, the thing it is waiting on
	WaitResource string
	// future: the cululative cpu time for this request
	CPUTime time.Duration
	// the elapsed time for this request
	ElapsedTime time.Duration
	// future: the cumulative number of physical reads for this request
	Reads int64
	// future: the cumulative number of physical writes for this request
	Writes int64
	// future: the cumulative number of logical reads for this request
	LogicalReads int64
	// future: the cumulative number of rows affected for this request
	RowCount int64
	// the query plan for this request formatted in json
	Plan string
	// the sql for this request
	SQL string
}

// Copy returns a copy of the ExecutionRequest passed
func (e *ExecutionRequest) Copy() ExecutionRequest {
	var elapsedTime time.Duration
	if !strings.EqualFold(e.Status, "complete") {
		elapsedTime = time.Since(e.StartTime)
	} else {
		elapsedTime = e.EndTime.Sub(e.StartTime)
	}

	return ExecutionRequest{
		RequestID:    e.RequestID,
		UserID:       e.UserID,
		StartTime:    e.StartTime,
		EndTime:      e.EndTime,
		Status:       e.Status,
		WaitType:     e.WaitType,
		WaitTime:     e.WaitTime,
		WaitResource: e.WaitResource,
		CPUTime:      e.CPUTime,
		ElapsedTime:  elapsedTime,
		SQL:          e.SQL,
		Plan:         e.Plan,
	}
}

// ExecutionRequestsAPI defines the API for storing, updating and querying internal state
// around (sql) execution requests
type ExecutionRequestsAPI interface {
	// add a request
	AddRequest(requestID string, userID string, startTime time.Time, sql string) error

	// update a request
	UpdateRequest(requestID string,
		endTime time.Time,
		status string,
		waitType string,
		waitTime time.Duration,
		waitResource string,
		cpuTime time.Duration,
		reads int64,
		writes int64,
		logicalReads int64,
		rowCount int64,
		plan string) error

	// list all the requests
	ListRequests() ([]ExecutionRequest, error)

	// get a specific request
	GetRequest(requestID string) (ExecutionRequest, error)
}

// SystemLayerAPI defines an api to allow access to internal FeatureBase state
type SystemLayerAPI interface {
	ExecutionRequests() ExecutionRequestsAPI
}
