package controller

import (
	"github.com/featurebasedb/featurebase/v3/dax"
)

// WorkerRegistry represents a service for managing Nodes. Note that this
// interface mirrors the dax.WorkerRegistry interface, but its methods take
// dax.Transactions rather than Contexts. That's because the dax version of this
// interface is meant to be a the API boundary, where this is an interface for
// use within the Controller.
type WorkerRegistry interface {
	AddWorker(dax.Transaction, *dax.Node) error
	Worker(dax.Transaction, dax.Address) (*dax.Node, error)
	RemoveWorker(dax.Transaction, dax.Address) error
	Workers(dax.Transaction) ([]*dax.Node, error)
}

// Ensure type implements interface.
var _ WorkerRegistry = &nopWorkerRegistry{}

// nopWorkerRegistry is a no-op implementation of the WorkerRegistry interface.
type nopWorkerRegistry struct{}

func NewNopWorkerRegistry() *nopWorkerRegistry {
	return &nopWorkerRegistry{}
}

func (n *nopWorkerRegistry) AddWorker(dax.Transaction, *dax.Node) error {
	return nil
}
func (n *nopWorkerRegistry) Worker(dax.Transaction, dax.Address) (*dax.Node, error) {
	return nil, nil
}
func (n *nopWorkerRegistry) RemoveWorker(dax.Transaction, dax.Address) error {
	return nil
}
func (n *nopWorkerRegistry) Workers(dax.Transaction) ([]*dax.Node, error) {
	return []*dax.Node{}, nil
}
