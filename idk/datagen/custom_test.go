package datagen

import (
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/idk"
)

func TestGetIDKFields(t *testing.T) {
	cc := &CustomConfig{
		Fields: []*GenField{
			{
				Name: "a",
				Type: "uint",
			},
			{
				Name: "b",
				Type: "int",
			},
		},
		IDKParams: IDKParams{
			Fields: map[string][]IngestField{
				"a": {
					{
						Type: "ID",
						Name: "a",
					},
					{
						Type: "Int",
						Name: "a2",
					},
					{
						Type:        "ID",
						Name:        "ID_ttl",
						TimeQuantum: "YMD",
						TTL:         "101s",
					},
					{
						Type:        "IDArray",
						Name:        "IDArray_ttl",
						TimeQuantum: "YMD",
						TTL:         "102s",
					},
					{
						Type:        "String",
						Name:        "String_ttl",
						TimeQuantum: "YMD",
						TTL:         "103s",
					},
					{
						Type:        "StringArray",
						Name:        "StringArray_ttl",
						TimeQuantum: "YMD",
						TTL:         "104s",
					},
				},
			},
		},
	}
	min := int64(0)
	max := int64(0)
	exp := &IDKAndGenFields{
		schema: []idk.Field{
			idk.IDField{
				NameVal:     "a",
				DestNameVal: "a",
			},
			idk.IntField{
				NameVal:     "a",
				DestNameVal: "a2",
			},
			idk.IDField{
				NameVal:     "a",
				DestNameVal: "ID_ttl",
				Quantum:     "YMD",
				TTL:         "101s",
			},
			idk.IDArrayField{
				NameVal:     "a",
				DestNameVal: "IDArray_ttl",
				Quantum:     "YMD",
				TTL:         "102s",
			},
			idk.StringField{
				NameVal:     "a",
				DestNameVal: "String_ttl",
				Quantum:     "YMD",
				TTL:         "103s",
			},
			idk.StringArrayField{
				NameVal:     "a",
				DestNameVal: "StringArray_ttl",
				Quantum:     "YMD",
				TTL:         "104s",
			},
			idk.IntField{
				NameVal:     "b",
				DestNameVal: "", // no dest because we take the default path
				Min:         &min,
				Max:         &max,
			},
		},
		genFields: []*GenField{
			{
				Name: "a",
				Type: "uint",
			},
			nil,
			nil,
			nil,
			nil,
			nil,
			{
				Name: "b",
				Type: "int",
			},
		},
	}

	igs, err := cc.GetIDKFields()
	if err != nil {
		t.Fatalf("getting IDK fields: %v", err)
	}

	if len(igs.schema) != len(exp.schema) ||
		len(igs.genFields) != len(exp.genFields) ||
		len(igs.schema) != len(igs.genFields) {
		t.Fatalf("mismatched lengths: %+v", igs)
	}
	for i := 0; i < len(igs.schema); i++ {
		gotSchema, expSchema := igs.schema[i], exp.schema[i]
		gotGenField, expGenField := igs.genFields[i], exp.genFields[i]
		if !idk.FieldsEqual(gotSchema, expSchema) {
			t.Fatalf("mismatched schemas at %d: got:\n%+v\nexp:\n%+v", i, gotSchema, expSchema)
		}
		if gotGenField == nil && expGenField == nil {
			// great
		} else if *gotGenField != *expGenField {
			t.Fatalf("mismatched genFields at %d: got:\n%+v\nexp:\n%+v", i, gotGenField, expGenField)
		}
	}
}

func TestNewCustomUnmarshalling(t *testing.T) {
	tmp, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatalf("creating temp file: %v", err)
	}
	defer os.Remove(tmp.Name())
	_, err = tmp.WriteString(`
fields:
  - name: "myfield"
    type: "timestamp"
    min_step_duration: "1s"
    min_date: 2013-10-16T22:34:43Z
    time_format: "unix"
    time_unit: "ms"
`)
	if err != nil {
		t.Fatalf("writing: %v", err)
	}
	cc := NewCustom(SourceGeneratorConfig{CustomConfig: tmp.Name()}).(*Custom)
	if cc.err != nil {
		t.Fatalf("creating Custom: %v", cc.err)
	}
	if cc.CustomConfig.Fields[0].minStepDuration != time.Second {
		t.Fatalf("wrong duration: %+v", cc.CustomConfig.Fields[0])
	}
	if cc.CustomConfig.Fields[0].MinDate.Hour() != 22 {
		t.Fatalf("wrong time: %s", cc.CustomConfig.Fields[0].MinDate)
	}
	if cc.CustomConfig.Fields[0].TimeUnit != idk.Millisecond {
		t.Fatalf("wrong time unit: %s", cc.CustomConfig.Fields[0].TimeUnit)
	}
	if cc.CustomConfig.Fields[0].TimeFormat != unixFormat {
		t.Fatalf("wrong time format: %s", cc.CustomConfig.Fields[0].TimeFormat)
	}
}

func TestIncreasingTimestampGenerator(t *testing.T) {
	cases := []struct {
		name string
		unit idk.Unit
		min  int64
		max  int64
	}{
		{
			name: "millis",
			unit: idk.Millisecond,
			max:  1136073600000 + 2001,
			min:  1136073600000,
		},
		{
			name: "days",
			unit: idk.Day,
			max:  13149,
			min:  13149,
		},
	}

	for _, testcase := range cases {
		t.Run(testcase.name, func(t *testing.T) {
			genField := &GenField{
				Type:            "timestamp",
				minStepDuration: time.Second,
				maxStepDuration: time.Second * 2,
				TimeFormat:      unixFormat,
				TimeUnit:        testcase.unit,
				Distribution:    "increasing",
				MinDate:         time.Date(2006, time.January, 1, 0, 0, 0, 0, time.UTC),
				MaxDate:         time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
			}

			r := rand.New(rand.NewSource(3))
			gen, err := genField.Generator(r)
			if err != nil {
				t.Fatalf("making generator: %v", err)
			}

			rec, err := gen.Generate(nil)
			if err != nil {
				t.Fatalf("generating: %v", err)
			}
			if recInt, ok := rec.(int64); !ok {
				t.Fatalf("expected int64, but got %v of %[1]T", recInt)
			} else if recInt < testcase.min || recInt > testcase.max {
				t.Fatalf("unexpected value for time: %v", recInt)
			}
		})
	}
}

func TestShiftingStringGenerator(t *testing.T) {
	// max < min
	if ssg, err := NewShiftingStringGenerator(&GenField{MinLen: 10, MaxLen: 9}, nil); err == nil {
		t.Fatalf("expected error, but got %+v", ssg)
	} else if !strings.Contains(err.Error(), "must be greater than or equal to") {
		t.Fatalf("unexpected error: %v", err)
	}

	// neg step
	if ssg, err := NewShiftingStringGenerator(&GenField{Step: -1}, nil); err == nil {
		t.Fatalf("expected error, but got %+v", ssg)
	} else if !strings.Contains(err.Error(), "'step' for shifting must be positive") {
		t.Fatalf("unexpected error: %v", err)
	}

	// min zero but max not
	if ssg, err := NewShiftingStringGenerator(&GenField{MaxLen: 7, Step: 2}, rand.New(rand.NewSource(1))); err != nil {
		t.Fatalf("unexpected error %v", err)
	} else if ssg.lenVariance != 0 {
		t.Fatalf("expected zero variance, but got: %+v", ssg)
	}

	// max zero but min not
	if ssg, err := NewShiftingStringGenerator(&GenField{MinLen: 7}, rand.New(rand.NewSource(1))); err != nil {
		t.Fatalf("unexpected error %v", err)
	} else if ssg.lenVariance != 0 {
		t.Fatalf("expected zero variance, but got: %+v", ssg)
	} else if ssg.step != 1 {
		t.Fatalf("expected step 0 but got: %+v", ssg)
	}

	ssg, err := NewShiftingStringGenerator(&GenField{MinLen: 5, MaxLen: 10, Cardinality: 4, Step: 20}, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}

	vals := make([]string, 0, 40)
	valMap := make(map[string]struct{})
	for i := 0; i < 20; i++ {
		val, err := ssg.Generate(nil)
		if err != nil {
			t.Fatalf("unexpected generation err: %v", err)
		}
		valStr := val.(string)
		vals = append(vals, valStr)
		valMap[valStr] = struct{}{}
		if len(valStr) < 5 || len(valStr) > 10 {
			t.Fatalf("got unexpected length val at position: %d, %v", i, vals)
		}
	}
	if len(valMap) != 4 {
		t.Fatalf("unexpected cardinality: %v", valMap)
	}
	for i := 20; i < 40; i++ {
		val, err := ssg.Generate(nil)
		if err != nil {
			t.Fatalf("unexpected generation err: %v", err)
		}
		valStr := val.(string)
		vals = append(vals, valStr)
		delete(valMap, valStr)
		if len(valStr) < 5 || len(valStr) > 10 {
			t.Fatalf("got unexpected length val at position: %d, %v", i, vals)
		}
	}
	if len(valMap) != 1 {
		t.Fatalf("should have a single value from the first 20 that isn't generated in second 20, but have valMap:\n%v\nvals:\n%v", valMap, vals)
	}
}
