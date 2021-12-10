package testhook_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/molecula/featurebase/v2/testhook"
)

func TestAuditor_CatchError(t *testing.T) {
	auditor := testhook.NewVerifyCloseAuditor(nil)
	var x, y int
	reg, err := auditor.Registry(&x)
	if err != nil {
		t.Fatalf("requesting registry: %v", err)
	}
	err = reg.Created(&x, nil)
	if err != nil {
		t.Fatalf("creating x: %v", err)
	}
	err = reg.Opened(&x, nil)
	if err != nil {
		t.Fatalf("opening x: %v", err)
	}
	err = reg.Seen(&y, nil)
	if err == nil {
		t.Fatalf("seeing unopened y: expected error, didn't get one")
	}
	err = reg.Seen(&x, nil)
	if err != nil {
		t.Fatalf("seeing opened x: unexpected error %v", err)
	}
	err = reg.Created(&y, nil)
	if err != nil {
		t.Fatalf("creating y: %v", err)
	}
	err = reg.Opened(&y, nil)
	if err != nil {
		t.Fatalf("opening y: %v", err)
	}
	err = reg.Closed(&x, nil)
	if err != nil {
		t.Fatalf("closing x: %v", err)
	}
	err = reg.Closed(&x, nil)
	if err == nil {
		t.Fatalf("double-closing x: expected error, didn't get one")
	}
	err = reg.Destroyed(&x, nil)
	if err != nil {
		t.Fatalf("destroying x: got unexpected error %v", err)
	}
	err, errs := auditor.FinalCheck()
	if err == nil {
		t.Fatalf("unclosed y not detected")
	}
	_ = errs
}

type ignoreLiveness struct {
	skippable *int
}

func (ign *ignoreLiveness) Live(o interface{}, _ *testhook.RegistryEntry) error {
	if ptr, ok := o.(*int); ok {
		if ptr == ign.skippable {
			return nil
		}
	}
	return errors.New("unexpected live object")
}

var _ testhook.RegistryHookLive = &ignoreLiveness{}

func TestAuditor_DiscardError(t *testing.T) {
	var x, y int
	iptr := reflect.TypeOf(&x)
	auditor := testhook.NewVerifyCloseAuditor(testhook.RegistryHooks{iptr: &ignoreLiveness{skippable: &y}})
	reg, err := auditor.Registry(&x)
	if err != nil {
		t.Fatalf("requesting registry: %v", err)
	}
	err = reg.Created(&x, nil)
	if err != nil {
		t.Fatalf("creating x: %v", err)
	}
	err = reg.Opened(&x, nil)
	if err != nil {
		t.Fatalf("opening x: %v", err)
	}
	err = reg.Seen(&y, nil)
	if err == nil {
		t.Fatalf("seeing unopened y: expected error, didn't get one")
	}
	err = reg.Seen(&x, nil)
	if err != nil {
		t.Fatalf("seeing opened x: unexpected error %v", err)
	}
	err = reg.Created(&y, nil)
	if err != nil {
		t.Fatalf("creating y: %v", err)
	}
	err = reg.Opened(&y, nil)
	if err != nil {
		t.Fatalf("opening y: %v", err)
	}
	err = reg.Closed(&x, nil)
	if err != nil {
		t.Fatalf("closing x: %v", err)
	}
	err = reg.Closed(&x, nil)
	if err == nil {
		t.Fatalf("double-closing x: expected error, didn't get one")
	}
	err = reg.Destroyed(&x, nil)
	if err != nil {
		t.Fatalf("destroying x: got unexpected error %v", err)
	}
	err, errs := auditor.FinalCheck()
	if err != nil {
		t.Fatalf("expected to skip y, instead got err %v, error list %v", err, errs)
	}
}

func TestAuditor_KeepError(t *testing.T) {
	var x, y, z int
	iptr := reflect.TypeOf(&x)
	auditor := testhook.NewVerifyCloseAuditor(testhook.RegistryHooks{iptr: &ignoreLiveness{skippable: &z}})
	reg, err := auditor.Registry(&x)
	if err != nil {
		t.Fatalf("requesting registry: %v", err)
	}
	err = reg.Created(&x, nil)
	if err != nil {
		t.Fatalf("creating x: %v", err)
	}
	err = reg.Opened(&x, nil)
	if err != nil {
		t.Fatalf("opening x: %v", err)
	}
	err = reg.Seen(&y, nil)
	if err == nil {
		t.Fatalf("seeing unopened y: expected error, didn't get one")
	}
	err = reg.Seen(&x, nil)
	if err != nil {
		t.Fatalf("seeing opened x: unexpected error %v", err)
	}
	err = reg.Created(&y, nil)
	if err != nil {
		t.Fatalf("creating y: %v", err)
	}
	err = reg.Opened(&y, nil)
	if err != nil {
		t.Fatalf("opening y: %v", err)
	}
	err = reg.Closed(&x, nil)
	if err != nil {
		t.Fatalf("closing x: %v", err)
	}
	err = reg.Closed(&x, nil)
	if err == nil {
		t.Fatalf("double-closing x: expected error, didn't get one")
	}
	err = reg.Destroyed(&x, nil)
	if err != nil {
		t.Fatalf("destroying x: %v", err)
	}
	err, errs := auditor.FinalCheck()
	if err == nil {
		t.Fatalf("undestroyed y not detected")
	}
	_ = errs
}

type failHook struct {
	calls int
}

func (f *failHook) Opened(i interface{}, kv testhook.KV, _ *testhook.RegistryEntry) error {
	f.calls++
	return errors.New("failHook always fails")
}

func (f *failHook) WasOpened(i interface{}, kv testhook.KV, _ *testhook.RegistryEntry, err error) error {
	f.calls++
	return errors.New("failHook always fails")
}

// Implement WasSeen but not Seen, so we can verify that the only-one
// case works in both directions.
func (f *failHook) WasSeen(i interface{}, kv testhook.KV, _ *testhook.RegistryEntry, err error) error {
	f.calls++
	return errors.New("failHook always fails")
}

func (f *failHook) Closed(i interface{}, kv testhook.KV, _ *testhook.RegistryEntry) error {
	f.calls++
	return errors.New("failHook always fails")
}

func (f *failHook) WasClosed(i interface{}, kv testhook.KV, _ *testhook.RegistryEntry, err error) error {
	f.calls++
	return errors.New("failHook always fails")
}

func (f *failHook) Live(i interface{}, ent *testhook.RegistryEntry) error {
	f.calls++
	return errors.New("failHook always fails")
}

type successHook struct {
	calls int
}

func (s *successHook) Opened(i interface{}, kv testhook.KV, _ *testhook.RegistryEntry) error {
	s.calls++
	return nil
}

func (s *successHook) WasOpened(i interface{}, kv testhook.KV, _ *testhook.RegistryEntry, err error) error {
	s.calls++
	return nil
}

func (s *successHook) Closed(i interface{}, kv testhook.KV, _ *testhook.RegistryEntry) error {
	s.calls++
	return nil
}

func (s *successHook) Seen(i interface{}, kv testhook.KV, _ *testhook.RegistryEntry) error {
	s.calls++
	return nil
}

// successHook allows an error to leak from WasClosed.
func (s *successHook) WasClosed(i interface{}, kv testhook.KV, _ *testhook.RegistryEntry, err error) error {
	s.calls++
	return err
}

func (s *successHook) Live(i interface{}, ent *testhook.RegistryEntry) error {
	s.calls++
	return errors.New("even successHook can fail sometimes")
}

func TestComposedHooks(t *testing.T) {
	s := &successHook{}
	f := &failHook{}
	sExp, fExp := 0, 0
	var err error
	checkExp := func(when string) {
		if s.calls != sExp {
			t.Fatalf("after %s, expected %d calls to successHook, got %d", when, sExp, s.calls)
		}
		if f.calls != fExp {
			t.Fatalf("after %s, expected %d calls to failHook, got %d", when, fExp, f.calls)
		}
	}
	combined := testhook.Compose(s, f)
	// Opened: we expect both opened calls to be hit, and the error from
	// the second to come back.
	err = combined.Opened(nil, nil, nil)
	sExp++
	fExp++
	checkExp("opened")
	if err == nil {
		t.Fatalf("composed hook, opened: didn't error")
	}
	if err.Error() != "failHook always fails" {
		t.Fatalf("composed hook, opened: expected failHook always fails, got %v", err)
	}

	// WasOpened: we expect the error from failHook to be overridden.
	err = combined.WasOpened(nil, nil, nil, nil)
	sExp++
	fExp++
	checkExp("wasOpened")
	if err != nil {
		t.Fatalf("composed hook, wasOpened: expected no error, got %v", err)
	}

	// Seen: nothing to call for failHook
	err = combined.Seen(nil, nil, nil)
	sExp++
	checkExp("seen")
	if err != nil {
		t.Fatalf("composed hook, seen: expected no error, got %v", err)
	}

	// WasSeen: nothing to call for successHook
	err = combined.WasSeen(nil, nil, nil, nil)
	fExp++
	checkExp("wasSeen")
	if err == nil {
		t.Fatalf("composed hook, wasSeen: expected error, didn't get it")
	}

	// WasClosed: expect both to get called, but successHook to leak the error up
	err = combined.WasClosed(nil, nil, nil, nil)
	sExp++
	fExp++
	checkExp("wasClosed")
	if err == nil {
		t.Fatalf("composed hook, wasClosed: expected error, didn't get it")
	}

	// Live: the failure from successHook (oops) should prevent failHook from
	// being called.
	err = combined.Live(nil, nil)
	sExp++
	checkExp("live")
	if err == nil {
		t.Fatalf("composed hook, live: expected error, didn't get it")
	}
}
