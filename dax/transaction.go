package dax

import "context"

type Transaction interface {
	Commit() error
	Context() context.Context
	Rollback() error
}
