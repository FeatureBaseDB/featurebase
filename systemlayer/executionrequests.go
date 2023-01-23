package systemlayer

import (
	"fmt"
	"sync"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
)

const (
	keepLastRequests = 2000
	truncateTextAt   = 4096
)

// ExecutionRequests is an internal struct that keeps a list of sql execution requests
// this data allows visbility into queries that have been run and are running
type ExecutionRequests struct {
	sync.RWMutex
	// requestsList being used a FIFO queue here
	// to track both number of stored requests and
	// which one to delete next (always 0)
	requestsList []string
	// the actual rquests being stored - we use a map
	// so we can look them up by request id which is a uuid
	requests map[string]*pilosa.ExecutionRequest
}

// Ensure type implements interface.
var _ pilosa.ExecutionRequestsAPI = (*ExecutionRequests)(nil)

func NewExecutionRequestsAPI() *ExecutionRequests {
	return &ExecutionRequests{
		requestsList: make([]string, 0),
		requests:     make(map[string]*pilosa.ExecutionRequest),
	}
}

// AddRequest adds a new request to the ExecutionRequests struct
func (e *ExecutionRequests) AddRequest(requestID string, userID string, startTime time.Time, sql string) error {
	e.Lock()
	defer e.Unlock()

	// we're going to add a new request so clear out oldest request if we've hit
	// the threshhold
	if len(e.requestsList) >= keepLastRequests {
		// get the oldest request and delete it
		delId := e.requestsList[0]
		delete(e.requests, delId)
		e.requestsList = e.requestsList[1:]
	}
	// add the request to the end
	e.requestsList = append(e.requestsList, requestID)
	// update the request
	_, ok := e.requests[requestID]
	if ok {
		return fmt.Errorf("request %s already exists", requestID)
	}
	if len(sql) > truncateTextAt {
		sql = sql[:truncateTextAt] //truncate to 4k, if we need more later, we'll worry about that later
	}
	e.requests[requestID] = &pilosa.ExecutionRequest{
		RequestID: requestID,
		UserID:    userID,
		StartTime: startTime,
		Status:    "running",
		SQL:       sql,
	}
	return nil
}

// UpdateRequest updates the values for a request in the ExecutionRequests struct
func (e *ExecutionRequests) UpdateRequest(requestID string,
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
	plan string) error {

	e.Lock()
	defer e.Unlock()

	request, ok := e.requests[requestID]
	if !ok {
		return fmt.Errorf("request %s not found", requestID)
	}
	if len(plan) > truncateTextAt {
		plan = plan[:truncateTextAt] //truncate to 4k, if we need more later, we'll worry about that later
	}
	request.EndTime = endTime
	request.Status = status
	request.WaitType = waitType
	request.WaitTime += waitTime
	request.WaitResource = waitResource
	request.CPUTime += cpuTime
	request.Reads += reads
	request.Writes += writes
	request.LogicalReads += logicalReads
	request.RowCount += rowCount
	request.Plan = plan

	return nil
}

// ListRequests returns the content of the ExecutionRequests struct as copies
func (e *ExecutionRequests) ListRequests() ([]pilosa.ExecutionRequest, error) {
	e.RLock()
	defer e.RUnlock()

	result := make([]pilosa.ExecutionRequest, len(e.requests))

	idx := 0
	for _, er := range e.requests {
		result[idx] = er.Copy()
		idx++
	}
	return result, nil
}

func (e *ExecutionRequests) GetRequest(requestID string) (pilosa.ExecutionRequest, error) {
	e.RLock()
	defer e.RUnlock()

	er, ok := e.requests[requestID]
	if !ok {
		return pilosa.ExecutionRequest{}, fmt.Errorf("request %s not found", requestID)
	}

	return er.Copy(), nil
}
