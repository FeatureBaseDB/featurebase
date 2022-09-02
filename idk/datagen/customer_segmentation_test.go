package datagen

import (
	"testing"
)

func TestPersonHobbies(t *testing.T) {
	p := NewPerson(SourceGeneratorConfig{})
	ps := p.Source(SourceConfig{endAt: 10})

	i := 0
	for i = 0; i < 10; i++ {
		rec, err := ps.Record()
		if err != nil {
			t.Fatalf("getting record: %v", err)
		}
		data := rec.Data()
		if dataStringSlice, ok := data[10].([]string); !ok {
			t.Fatalf("expected string slice, but got type:%T, data:%[1]v", data[10])
		} else if len(dataStringSlice) > 0 {
			break // we just want to see one that has data in it, then we'll call that success
		}
	}
	if i == 10 {
		t.Fatalf("never saw any generated hobbies (slice was always empty)")
	}
}
