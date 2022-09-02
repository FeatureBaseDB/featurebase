// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package testhook

import (
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"time"
)

// KV represents a key/value mapping. You don't need to use the type
// for anything, it's just to make typing easier.
type KV map[string]interface{}

// Registry represents a set of known objects of a given common type.
// A typical implementation might maintain a map of objects it's seen which
// haven't been deleted. A minimal implementation just does nothing.
//
// A Registry may implement reference-count semantics, or may treat
// reopening of a still-open object as an error.
//
// All objects provided to a registry should have the same type.
type Registry interface {
	Created(interface{}, KV) error                 // Object has been created, and must be destroyed later.
	Opened(interface{}, KV) error                  // Object has been opened, and must be closed later.
	Seen(interface{}, KV) error                    // Object has been interacted with, and should be between an open and a close.
	Closed(interface{}, KV) error                  // Object has been closed, and must have been opened previously.
	Destroyed(interface{}, KV) error               // Object has been destroyed
	Live() (map[interface{}]*RegistryEntry, error) // Currently live objects
}

// RegistryHook is a generic interface, but any real implementation should
// implement at least one of RegistryHookPreOpen, RegistryHookPostOpen,
// etcetera. Pre-hooks are called before any error checks by the registry,
// post-hooks are called with after those error checks, and are passed the
// (possibly nil) error that would be returned at that point. If the post-hook
// overrides the error, the registry continues with operations as though it
// hadn't occurred, which may be a very bad idea.
type RegistryHook interface{}

type RegistryHookPreCreate interface {
	Created(interface{}, KV, *RegistryEntry) error
}

type RegistryHookPostCreate interface {
	WasCreated(interface{}, KV, *RegistryEntry, error) error
}

type RegistryHookPreOpen interface {
	Opened(interface{}, KV, *RegistryEntry) error
}

type RegistryHookPostOpen interface {
	WasOpened(interface{}, KV, *RegistryEntry, error) error
}

type RegistryHookPreSee interface {
	Seen(interface{}, KV, *RegistryEntry) error
}

type RegistryHookPostSee interface {
	WasSeen(interface{}, KV, *RegistryEntry, error) error
}

type RegistryHookPreClose interface {
	Closed(interface{}, KV, *RegistryEntry) error
}

type RegistryHookPostClose interface {
	WasClosed(interface{}, KV, *RegistryEntry, error) error
}

type RegistryHookPreDestroy interface {
	Destroyed(interface{}, KV, *RegistryEntry) error
}

type RegistryHookPostDestroy interface {
	WasDestroyed(interface{}, KV, *RegistryEntry, error) error
}

// If a registry's hooks implement RegistryHookLive, Live() should
// return only those entries for which a non-nil error was returned, with the
// error inserted in the RegistryEntry.
type RegistryHookLive interface {
	Live(interface{}, *RegistryEntry) error
}

// RegistryHooks represents a set of registry hook values to use for
// registries, corresponding to different types.
type RegistryHooks map[reflect.Type]RegistryHook

// RegistryEntry represents the data we might have about an entry. Every
// entry in it could be zero-valued in some implementations
type RegistryEntry struct {
	Error     error
	Stack     []byte
	Stamp     time.Time
	Data      KV
	OpenCount int
}

// NopRegistry doesn't do anything; it exists to fit the interface but not
// consume resources.
type NopRegistry struct{}

var _ Registry = &NopRegistry{}

func (*NopRegistry) Created(interface{}, KV) error                 { return nil }
func (*NopRegistry) Opened(interface{}, KV) error                  { return nil }
func (*NopRegistry) Seen(interface{}, KV) error                    { return nil }
func (*NopRegistry) Closed(interface{}, KV) error                  { return nil }
func (*NopRegistry) Destroyed(interface{}, KV) error               { return nil }
func (*NopRegistry) Live() (map[interface{}]*RegistryEntry, error) { return nil, nil }

func NewNopRegistry() *NopRegistry {
	return &NopRegistry{}
}

// SimpleRegistry asserts that objects are created, then opened, then
// possibly seen, then closed, then destroyed, and that they are not
// opened more than once at a time, or destroyed while open. As a
// convenience feature for users whose use cases might rely on this,
// it will actually accept a new item being opened without being
// previously created; to prevent this, use a PreOpen hook that checks
// for a nil *RegistryEntry. The default Live check will report only
// objects with an open count other than 0, but if you provide a Live
// hook, you can return errors for objects still existing.
type SimpleRegistry struct {
	hooks   RegistryHook
	entries map[interface{}]*RegistryEntry
	mu      sync.Mutex
}

var _ Registry = &SimpleRegistry{}

func NewSimpleRegistry(hooks RegistryHook) *SimpleRegistry {
	return &SimpleRegistry{entries: map[interface{}]*RegistryEntry{}, hooks: hooks}
}

func (s *SimpleRegistry) Created(o interface{}, kv KV) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing := s.entries[o]
	if hook, ok := s.hooks.(RegistryHookPreCreate); ok {
		if err := hook.Created(o, kv, existing); err != nil {
			return err
		}
	}
	if existing != nil {
		err = fmt.Errorf("object %T:%v previously registered at %v", o, o, existing.Stamp)
	} else {
		existing = &RegistryEntry{
			Stack: debug.Stack(),
			Data:  kv,
			Stamp: time.Now(),
		}
	}
	if hook, ok := s.hooks.(RegistryHookPostCreate); ok {
		err = hook.WasCreated(o, kv, existing, err)
	}
	if err != nil {
		return err
	}
	// if you overrode the error, or there wasn't one and you didn't
	// create one, we now stash the entry.
	s.entries[o] = existing
	return nil
}

func (s *SimpleRegistry) Opened(o interface{}, kv KV) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing := s.entries[o]
	if hook, ok := s.hooks.(RegistryHookPreOpen); ok {
		if err := hook.Opened(o, kv, existing); err != nil {
			return err
		}
	}
	if existing == nil {
		existing = &RegistryEntry{
			Stack: debug.Stack(),
			Data:  kv,
			Stamp: time.Now(),
		}
		s.entries[o] = existing
	}
	existing.OpenCount++
	if existing.OpenCount > 1 {
		err = fmt.Errorf("object %T:%v opened %d times", o, o, existing.OpenCount)
	}
	if hook, ok := s.hooks.(RegistryHookPostOpen); ok {
		err = hook.WasOpened(o, kv, existing, err)
	}
	if err != nil {
		if existing != nil {
			existing.OpenCount--
		}
		return err
	}
	return nil
}

func (s *SimpleRegistry) Seen(o interface{}, kv KV) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing := s.entries[o]
	if hook, ok := s.hooks.(RegistryHookPreSee); ok {
		if err = hook.Seen(o, kv, existing); err != nil {
			return err
		}
	}
	if existing == nil {
		err = fmt.Errorf("object %T:%v seen but not previously registered", o, o)
	}
	if hook, ok := s.hooks.(RegistryHookPostSee); ok {
		err = hook.WasSeen(o, kv, existing, err)
	}
	return err
}

func (s *SimpleRegistry) Closed(o interface{}, kv KV) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing := s.entries[o]
	if hook, ok := s.hooks.(RegistryHookPreClose); ok {
		if err = hook.Closed(o, kv, existing); err != nil {
			return err
		}
	}
	if existing == nil {
		err = fmt.Errorf("object %T:%v closed but not previously registered", o, o)
	} else {
		existing.OpenCount--
		if existing.OpenCount < 0 {
			err = fmt.Errorf("object %T:%v closed more often than it was open: %d", o, o, existing.OpenCount)
		}
	}
	if hook, ok := s.hooks.(RegistryHookPostClose); ok {
		err = hook.WasClosed(o, kv, existing, err)
	}
	if err != nil {
		// if a close "failed", we don't want to count it as being closed.
		if existing != nil {
			existing.OpenCount++
		}
		return err
	}
	return nil
}

func (s *SimpleRegistry) Destroyed(o interface{}, kv KV) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing := s.entries[o]
	if hook, ok := s.hooks.(RegistryHookPreDestroy); ok {
		if err = hook.Destroyed(o, kv, existing); err != nil {
			return err
		}
	}
	if existing == nil {
		err = fmt.Errorf("object %T:%v destroyed but not previously registered", o, o)
	} else if existing.OpenCount != 0 {
		err = fmt.Errorf("object %T:%v destroyed while open count is %d", o, o, existing.OpenCount)
	}
	if hook, ok := s.hooks.(RegistryHookPostDestroy); ok {
		err = hook.WasDestroyed(o, kv, existing, err)
	}
	if err != nil {
		fmt.Printf("destroyed error: %v\n", err)
		return err
	}
	// remove the entry from the list.
	delete(s.entries, o)
	return nil
}

func (s *SimpleRegistry) Live() (results map[interface{}]*RegistryEntry, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	results = make(map[interface{}]*RegistryEntry)
	if hook, ok := s.hooks.(RegistryHookLive); ok {
		for k, v := range s.entries {
			v.Error = hook.Live(k, v)
			if v.Error != nil {
				results[k] = v
			}
		}
	} else {
		for k, v := range s.entries {
			if v.OpenCount != 0 {
				v.Error = fmt.Errorf("open count %d", v.OpenCount)
				results[k] = v
			}
		}
	}
	return results, nil
}

type PreHook func(interface{}, KV, *RegistryEntry) error
type PostHook func(interface{}, KV, *RegistryEntry, error) error
type LiveHook func(interface{}, *RegistryEntry) error

// PluggableRegistry is a registry which takes dynamically-generated functions,
// and calls them if they're not nil.
type PluggableRegistry struct {
	implCreated      PreHook
	implWasCreated   PostHook
	implOpened       PreHook
	implWasOpened    PostHook
	implSeen         PreHook
	implWasSeen      PostHook
	implClosed       PreHook
	implWasClosed    PostHook
	implDestroyed    PreHook
	implWasDestroyed PostHook
	implLive         LiveHook
}

func (p *PluggableRegistry) Created(i interface{}, kv KV, ent *RegistryEntry) error {
	if p.implCreated != nil {
		return p.implCreated(i, kv, ent)
	}
	return nil
}

func (p *PluggableRegistry) WasCreated(i interface{}, kv KV, ent *RegistryEntry, err error) error {
	if p.implWasCreated != nil {
		return p.implWasCreated(i, kv, ent, err)
	}
	return nil
}

func (p *PluggableRegistry) Opened(i interface{}, kv KV, ent *RegistryEntry) error {
	if p.implOpened != nil {
		return p.implOpened(i, kv, ent)
	}
	return nil
}

func (p *PluggableRegistry) WasOpened(i interface{}, kv KV, ent *RegistryEntry, err error) error {
	if p.implWasOpened != nil {
		return p.implWasOpened(i, kv, ent, err)
	}
	return nil
}

func (p *PluggableRegistry) Seen(i interface{}, kv KV, ent *RegistryEntry) error {
	if p.implSeen != nil {
		return p.implSeen(i, kv, ent)
	}
	return nil
}

func (p *PluggableRegistry) WasSeen(i interface{}, kv KV, ent *RegistryEntry, err error) error {
	if p.implWasSeen != nil {
		return p.implWasSeen(i, kv, ent, err)
	}
	return nil
}

func (p *PluggableRegistry) Closed(i interface{}, kv KV, ent *RegistryEntry) error {
	if p.implClosed != nil {
		return p.implClosed(i, kv, ent)
	}
	return nil
}

func (p *PluggableRegistry) WasClosed(i interface{}, kv KV, ent *RegistryEntry, err error) error {
	if p.implWasClosed != nil {
		return p.implWasClosed(i, kv, ent, err)
	}
	return nil
}

func (p *PluggableRegistry) Destroyed(i interface{}, kv KV, ent *RegistryEntry) error {
	if p.implDestroyed != nil {
		return p.implDestroyed(i, kv, ent)
	}
	return nil
}

func (p *PluggableRegistry) WasDestroyed(i interface{}, kv KV, ent *RegistryEntry, err error) error {
	if p.implWasDestroyed != nil {
		return p.implWasDestroyed(i, kv, ent, err)
	}
	return nil
}

func (p *PluggableRegistry) Live(i interface{}, ent *RegistryEntry) error {
	if p.implLive != nil {
		return p.implLive(i, ent)
	}
	return nil
}

func composePreHooks(fns ...PreHook) PreHook {
	if len(fns) == 0 {
		return nil
	}
	if len(fns) == 1 {
		return fns[0]
	}
	return func(i interface{}, kv KV, ent *RegistryEntry) error {
		for _, fn := range fns {
			err := fn(i, kv, ent)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func composePostHooks(fns ...PostHook) PostHook {
	if len(fns) == 0 {
		return nil
	}
	if len(fns) == 1 {
		return fns[0]
	}
	return func(i interface{}, kv KV, ent *RegistryEntry, err error) error {
		// We run the functions in reverse order, so the last
		// hook added has the option of overriding a lower hook's
		// opinion.
		for i := range fns {
			fn := fns[len(fns)-1-i]
			err = fn(i, kv, ent, err)
		}
		return err
	}
}

func composeLiveHooks(fns ...LiveHook) LiveHook {
	if len(fns) == 0 {
		return nil
	}
	if len(fns) == 1 {
		return fns[0]
	}
	return func(i interface{}, ent *RegistryEntry) error {
		for _, fn := range fns {
			err := fn(i, ent)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// Compose takes a list of RegistryHook objects, and combines their hook
// functions into a unified registry. For Pre hooks and Live hooks, the
// functions are called in the order they were provided to this function,
// and the first one to return an error causes the remainder to be
// skipped; for Post hooks, they are called in the opposite order, and
// the whole list continues to be called, with each function getting passed
// the error (or non-error) value returned by the previous one.
func Compose(hooks ...RegistryHook) *PluggableRegistry {
	var created, opened, seen, closed, destroyed []PreHook
	var wasCreated, wasOpened, wasSeen, wasClosed, wasDestroyed []PostHook
	var live []LiveHook
	for _, h := range hooks {
		if hook, ok := h.(RegistryHookPreCreate); ok {
			created = append(created, hook.Created)
		}
		if hook, ok := h.(RegistryHookPostCreate); ok {
			wasCreated = append(wasCreated, hook.WasCreated)
		}
		if hook, ok := h.(RegistryHookPreOpen); ok {
			opened = append(opened, hook.Opened)
		}
		if hook, ok := h.(RegistryHookPostOpen); ok {
			wasOpened = append(wasOpened, hook.WasOpened)
		}
		if hook, ok := h.(RegistryHookPreSee); ok {
			seen = append(seen, hook.Seen)
		}
		if hook, ok := h.(RegistryHookPostSee); ok {
			wasSeen = append(wasSeen, hook.WasSeen)
		}
		if hook, ok := h.(RegistryHookPreClose); ok {
			closed = append(closed, hook.Closed)
		}
		if hook, ok := h.(RegistryHookPostClose); ok {
			wasClosed = append(wasClosed, hook.WasClosed)
		}
		if hook, ok := h.(RegistryHookPreDestroy); ok {
			destroyed = append(destroyed, hook.Destroyed)
		}
		if hook, ok := h.(RegistryHookPostDestroy); ok {
			wasDestroyed = append(wasDestroyed, hook.WasDestroyed)
		}
		if hook, ok := h.(RegistryHookLive); ok {
			live = append(live, hook.Live)
		}
	}
	return &PluggableRegistry{
		implCreated:      composePreHooks(created...),
		implWasCreated:   composePostHooks(wasCreated...),
		implOpened:       composePreHooks(opened...),
		implWasOpened:    composePostHooks(wasOpened...),
		implSeen:         composePreHooks(seen...),
		implWasSeen:      composePostHooks(wasSeen...),
		implClosed:       composePreHooks(closed...),
		implWasClosed:    composePostHooks(wasClosed...),
		implDestroyed:    composePreHooks(destroyed...),
		implWasDestroyed: composePostHooks(wasDestroyed...),
		implLive:         composeLiveHooks(live...),
	}
}
