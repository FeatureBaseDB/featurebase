// Copyright 2020 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import (
	"fmt"
	"reflect"

	"github.com/molecula/featurebase/v2/testhook"
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
