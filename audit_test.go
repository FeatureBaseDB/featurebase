// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa_test

import (
	"fmt"
	"os"
	"reflect"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/testhook"
)

// AuditLeaksOn is a global switch to turn on resource
// leak checking at the end of a test run.
var AuditLeaksOn = true

// for tests, we use a single shared auditor used by all of the holders.
var globalTestAuditor = testhook.NewVerifyCloseAuditor(testHooks)

// These audit hooks are desireable during testing, but not in
// production.
type auditorIndexHooks struct{}
type auditorFieldHooks struct{}
type auditorHolderHooks struct{}

// static type checking
var _ testhook.RegistryHookLive = &auditorIndexHooks{}
var _ testhook.RegistryHookLive = &auditorFieldHooks{}
var _ testhook.RegistryHookPostDestroy = &auditorHolderHooks{}
var _ testhook.RegistryHookLive = &auditorHolderHooks{}

var testHooks = map[reflect.Type]testhook.RegistryHook{
	reflect.TypeOf((*pilosa.Index)(nil)):  &auditorIndexHooks{},
	reflect.TypeOf((*pilosa.Field)(nil)):  &auditorFieldHooks{},
	reflect.TypeOf((*pilosa.Holder)(nil)): &auditorHolderHooks{},
}

func init() {
	if !AuditLeaksOn {
		return
	}
	for k, v := range pilosa.GetInternalTestHooks() {
		testHooks[k] = v
	}
	testhook.RegisterPreTestHook(func() error {
		pilosa.NewAuditor = NewTestAuditor
		return nil
	})
	testhook.RegisterPostTestHook(func() error {
		err, errs := globalTestAuditor.FinalCheck()
		if err != nil {
			for i, e := range errs {
				fmt.Fprintf(os.Stderr, "[%d]: %v\n", i, e)
			}
		}
		return err
	})
}

func NewTestAuditor() testhook.Auditor {
	return globalTestAuditor
}

func (*auditorIndexHooks) Live(o interface{}, entry *testhook.RegistryEntry) error {
	if entry != nil && entry.OpenCount != 0 {
		return fmt.Errorf("index %s still open", o.(*pilosa.Index).Name())
	}
	return nil
}

func (*auditorFieldHooks) Live(o interface{}, entry *testhook.RegistryEntry) error {
	if entry != nil && entry.OpenCount != 0 {
		return fmt.Errorf("field %s still open", o.(*pilosa.Field).Name())
	}
	return nil
}

func (*auditorHolderHooks) WasDestroyed(o interface{}, kv testhook.KV, ent *testhook.RegistryEntry, err error) error {
	path := o.(*pilosa.Holder).Path()
	if path == "" {
		fmt.Fprintf(os.Stderr, "OOPS: trying to destroy a holder with no path! created: %s\n",
			ent.Stack)
	} else {
		os.RemoveAll(o.(*pilosa.Holder).Path())
	}
	return err
}

func (*auditorHolderHooks) Live(o interface{}, entry *testhook.RegistryEntry) error {
	if entry != nil && entry.OpenCount != 0 {
		return fmt.Errorf("holder %s still open", o.(*pilosa.Holder).Path())
	}
	return nil
}
