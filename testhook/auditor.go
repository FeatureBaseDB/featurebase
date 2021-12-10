// Copyright 2021 Molecula Corp. All rights reserved.
package testhook

//TODO: Check() and FinalCheck() should return error as the last argument

import (
	"fmt"
	"reflect"
	"sync"
)

// Auditor represents a thing which knows how to audit events. For instance,
// it can check on when things are accessed, or when they were opened, or
// whether opened objects are later closed.
type Auditor interface {

	// Registry yields a registry for objects of this type.
	// multiple calls with the same object type yield the same registry.
	Registry(interface{}) (Registry, error)
	// Check performs any error-checking that can be done during
	// usage.
	Check() (error, []error)
	// FinalCheck performs any error-checking that makes sense only
	// after all operations are supposed to be complete, such as
	// verifying that opened objects have been closed.
	FinalCheck() (error, []error)
}

// Created(a, o, kv) is shorthand for a.Registry(o).Created(o, kv) plus
// the error checking inside that.
func Created(a Auditor, o interface{}, kv KV) error {
	r, err := a.Registry(o)
	if err != nil {
		return err
	}
	return r.Created(o, kv)
}

// Opened(a, o, kv) is shorthand for a.Registry(o).Opened(o, kv) plus
// the error checking inside that.
func Opened(a Auditor, o interface{}, kv KV) error {
	r, err := a.Registry(o)
	if err != nil {
		return err
	}
	return r.Opened(o, kv)
}

// Closed(a, o, kv) is shorthand for a.Registry(o).Closed(o, kv) plus
// the error checking inside that.
func Closed(a Auditor, o interface{}, kv KV) error {
	r, err := a.Registry(o)
	if err != nil {
		return err
	}
	return r.Closed(o, kv)
}

// Destroyed(a, o, kv) is shorthand for a.Registry(o).Destroyed(o, kv) plus
// the error checking inside that.
func Destroyed(a Auditor, o interface{}, kv KV) error {
	r, err := a.Registry(o)
	if err != nil {
		return err
	}
	return r.Destroyed(o, kv)
}

// Seen(a, o, kv) is shorthand for a.Registry(o).Seen(o, kv) plus
// the error checking inside that.
func Seen(a Auditor, o interface{}, kv KV) error {
	r, err := a.Registry(o)
	if err != nil {
		return err
	}
	return r.Seen(o, kv)
}

// NopAuditor doesn't do anything.
type NopAuditor struct{}

func (*NopAuditor) Registry(interface{}) (Registry, error) {
	return NewNopRegistry(), nil
}

func (*NopAuditor) Check() (error, []error) {
	return nil, nil
}

func (*NopAuditor) FinalCheck() (error, []error) {
	return nil, nil
}

func NewNopAuditor() *NopAuditor {
	return &NopAuditor{}
}

// VerifyCloseAuditor provides registries which it will check for things
// being closed.
type VerifyCloseAuditor struct {
	registries map[reflect.Type]Registry
	hooks      RegistryHooks
	regMu      sync.Mutex
}

func (v *VerifyCloseAuditor) Registry(o interface{}) (Registry, error) {
	t := reflect.TypeOf(o)
	v.regMu.Lock()
	defer v.regMu.Unlock()
	if exists, ok := v.registries[t]; ok {
		return exists, nil
	}
	reg := NewSimpleRegistry(v.hooks[t])
	v.registries[t] = reg
	return reg, nil
}

func (*VerifyCloseAuditor) Check() (error, []error) {
	return nil, nil
}

func (v *VerifyCloseAuditor) FinalCheck() (error, []error) {
	v.regMu.Lock()
	defer v.regMu.Unlock()
	var errs []error
	for t, reg := range v.registries {
		typeName := t.String()
		live, err := reg.Live()
		if err != nil {
			errs = append(errs, fmt.Errorf("registry[%s]: retrieving live list: %v",
				typeName, err))
			continue
		}
		if len(live) > 0 {
			for addr, entry := range live {
				if entry.Error != nil {
					errs = append(errs, fmt.Errorf("%v: item created at %v, stack %s",
						entry.Error, entry.Stamp, entry.Stack))
				} else {
					errs = append(errs, fmt.Errorf("live item found at %p, created at %v, stack %s",
						addr, entry.Stamp, entry.Stack))
				}
				if entry.Data["stack"] != nil {
					errs = append(errs, fmt.Errorf("stashed stack: %s", entry.Data["stack"]))
				}
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("final check: %d error(s)", len(errs)), errs
	}
	return nil, nil
}

func NewVerifyCloseAuditor(hooks RegistryHooks) *VerifyCloseAuditor {
	return &VerifyCloseAuditor{registries: map[reflect.Type]Registry{}, hooks: hooks}
}
