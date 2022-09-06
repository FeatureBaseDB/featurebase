// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"fmt"
	"reflect"

	"github.com/featurebasedb/featurebase/v3/testhook"
)

// These audit hooks are desireable during testing, but not in
// production.
type auditorViewHooks struct{}
type auditorFragmentHooks struct{}

// static type checks
var _ testhook.RegistryHookLive = &auditorViewHooks{}
var _ testhook.RegistryHookLive = &auditorFragmentHooks{}

func (*auditorViewHooks) Live(o interface{}, entry *testhook.RegistryEntry) error {
	if entry != nil && entry.OpenCount != 0 {
		return fmt.Errorf("view %s still open", o.(*view).name)
	}
	return nil
}

func (*auditorFragmentHooks) Live(o interface{}, entry *testhook.RegistryEntry) error {
	if entry != nil && entry.OpenCount != 0 {
		return fmt.Errorf("fragment %s still open", o.(*fragment).path())
	}
	return nil
}

func GetInternalTestHooks() testhook.RegistryHooks {
	return map[reflect.Type]testhook.RegistryHook{
		reflect.TypeOf((*view)(nil)):     &auditorViewHooks{},
		reflect.TypeOf((*fragment)(nil)): &auditorFragmentHooks{},
	}
}
