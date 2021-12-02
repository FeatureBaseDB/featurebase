package pilosa

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestTranslateIndexDbOnNilIndex(t *testing.T) {
	api := API{}
	api.holder = &Holder{}
	r := strings.NewReader("not important tbh")
	err := api.TranslateIndexDB(context.Background(), "nonExistentIndex", 0, r)
	expected := fmt.Errorf("index %q not found", "nonExistentIndex")
	if !reflect.DeepEqual(err, expected) {
		t.Fatalf("expected '%#v', got '%#v'", expected, err)
	}
}

func TestTranslateIndexDbOnNilTranslateStore(t *testing.T) {
	api := API{}
	indexes := make(map[string]*Index)
	indexes["index"] = &Index{name: "index"}
	api.holder = &Holder{indexes: indexes}
	r := strings.NewReader("not important tbh")
	err := api.TranslateIndexDB(context.Background(), "index", 0, r)
	expected := fmt.Errorf("index %q has no translate store", "index")
	if !reflect.DeepEqual(err, expected) {
		t.Fatalf("expected '%#v', got '%#v'", expected, err)
	}
}

func TestTranslateFieldDbOnNilIndex(t *testing.T) {
	api := API{}
	api.holder = &Holder{}
	r := strings.NewReader("not important tbh")
	err := api.TranslateFieldDB(context.Background(), "nonExistentIndex", "field", r)
	expected := fmt.Errorf("index %q not found", "nonExistentIndex")
	if !reflect.DeepEqual(err, expected) {
		t.Fatalf("expected '%#v', got '%#v'", expected, err)
	}
}

func TestTranslateFieldDbOnNilField(t *testing.T) {
	api := API{}
	indexes := make(map[string]*Index)
	indexes["index"] = &Index{name: "index"}
	api.holder = &Holder{indexes: indexes}
	r := strings.NewReader("not important tbh")
	err := api.TranslateFieldDB(context.Background(), "index", "nonExistentField", r)
	expected := fmt.Errorf("field %q/%q not found", "index", "nonExistentField")
	if !reflect.DeepEqual(err, expected) {
		t.Fatalf("expected '%#v', got '%#v'", expected, err)
	}
}

func TestTranslateFieldDbOnNilFieldWithFieldName_keys(t *testing.T) {
	api := API{}
	indexes := make(map[string]*Index)
	indexes["index"] = &Index{name: "index"}
	api.holder = &Holder{indexes: indexes}
	r := strings.NewReader("not important tbh")
	err := api.TranslateFieldDB(context.Background(), "index", "_keys", r)
	if err != nil {
		t.Fatalf("expected 'nil', got '%#v'", err)
	}
}

func TestTranslateFieldDbOnNilTranslateStore(t *testing.T) {
	api := API{}
	indexes := make(map[string]*Index)
	fields := make(map[string]*Field)
	fields["field"] = &Field{}
	indexes["index"] = &Index{name: "index", fields: fields}
	api.holder = &Holder{indexes: indexes}
	r := strings.NewReader("not important tbh")
	err := api.TranslateFieldDB(context.Background(), "index", "field", r)
	expected := fmt.Errorf("field %q/%q has no translate store", "index", "field")
	if !reflect.DeepEqual(err, expected) {
		t.Fatalf("expected '%#v', got '%#v'", expected, err)
	}
}
