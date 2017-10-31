package pilosa

// SecurityManager provides the ability to limit access to restricted endpoints
// during cluster configuration
type SecurityManager interface {
	SetRestricted()
	SetNormal()
}

// DefaultSecurityManager provides a no-op implimentation of the SecurityManager interface
type DefaultSecurityManager struct {
}

// SetRestricted no-op
func (sdm *DefaultSecurityManager) SetRestricted() {

}

// SetNormal no-op
func (sdm *DefaultSecurityManager) SetNormal() {

}
