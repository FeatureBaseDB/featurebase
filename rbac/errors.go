package rbac

import "fmt"

type RBACError struct {
	err string
}

func (e RBACError) Error() string {
	return fmt.Sprintf("RBACError: %s", e.err)
}
