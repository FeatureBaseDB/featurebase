package pilosa

// SecurityManager provides the ability to limit access to restricted endpoints
// during cluster configuration.
type SecurityManager interface {
	SetRestricted()
	SetNormal()
}

// NopSecurityManager provides a no-op implementation of the SecurityManager interface.
type NopSecurityManager struct {
}

// SetRestricted no-op.
func (sdm *NopSecurityManager) SetRestricted() {}

// SetNormal no-op.
func (sdm *NopSecurityManager) SetNormal() {}
