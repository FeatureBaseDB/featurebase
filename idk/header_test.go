package idk

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/logger"
)

func TestHeaderToField(t *testing.T) {
	seven := int64(7)
	tests := []struct {
		name   string
		input  string
		exp    Field
		expErr string
		expLog string
	}{
		{
			name:   "empty",
			input:  "",
			expErr: ErrNoFieldSpec.Error(),
		},
		{
			name:   "no name",
			input:  "__String",
			expErr: "field '__String' has no sourceName",
		},
		{
			name:   "justname",
			input:  "blah",
			expErr: ErrNoFieldSpec.Error(),
		},
		{
			name:   "justname__",
			input:  "blah__",
			expErr: "unknown field",
		},
		{
			name:   "unknown field",
			input:  "blah__Ztring",
			expErr: "unknown field",
		},
		{
			name:  "string",
			input: "a__String",
			exp:   StringField{NameVal: "a", DestNameVal: "a"},
		},
		{
			name:  "string-mutex-f",
			input: "a__String_F",
			exp:   StringField{NameVal: "a", DestNameVal: "a"},
		},
		{
			name:  "string-mutex",
			input: "a__String_T",
			exp:   StringField{NameVal: "a", DestNameVal: "a", Mutex: true},
		},
		{
			name:  "string-quantum",
			input: "a__String_F_YM",
			exp:   StringField{NameVal: "a", DestNameVal: "a", Mutex: false, Quantum: "YM"},
		},
		{
			name:  "string-ttl",
			input: "a__String_F_YM_30s",
			exp:   StringField{NameVal: "a", DestNameVal: "a", Mutex: false, Quantum: "YM", TTL: "30s"},
		},
		{
			name:   "string-extra_arg",
			input:  "a__String_F_YM_30s_Z",
			exp:    StringField{NameVal: "a", DestNameVal: "a", Mutex: false, Quantum: "YM", TTL: "30s"},
			expLog: "ignoring extra arguments to StringField a__String_F_YM_30s_Z",
		},
		{
			name:   "unknown arg",
			input:  "a__String_Z",
			expErr: "can't interpret 'Z' for StringField.Mutex for field 'a'",
		},
		{
			name:  "bool",
			input: "z__Bool",
			exp:   BoolField{NameVal: "z", DestNameVal: "z"},
		},
		{
			name:   "bool-extra",
			input:  "z__Bool_extra_2",
			exp:    BoolField{NameVal: "z", DestNameVal: "z"},
			expLog: "ignoring extra arguments to BoolField",
		},
		{
			name:  "id",
			input: "myname__ID",
			exp:   IDField{NameVal: "myname", DestNameVal: "myname"},
		},
		{
			name:  "id-mutex",
			input: "z__ID_T",
			exp:   IDField{NameVal: "z", DestNameVal: "z", Mutex: true},
		},
		{
			name:  "id-quantum",
			input: "z__ID_F_YMD",
			exp:   IDField{NameVal: "z", DestNameVal: "z", Mutex: false, Quantum: "YMD"},
		},
		{
			name:  "id-ttl",
			input: "z__ID_F_YMD_30s",
			exp:   IDField{NameVal: "z", DestNameVal: "z", Mutex: false, Quantum: "YMD", TTL: "30s"},
		},
		{
			name:   "id-extra_arg",
			input:  "z__ID_F_YMD_30s_Z",
			exp:    IDField{NameVal: "z", DestNameVal: "z", Mutex: false, Quantum: "YMD", TTL: "30s"},
			expLog: "ignoring extra arguments to IDField z__ID_F_YMD_30s_Z",
		},
		{
			name:  "int",
			input: "myname__Int",
			exp:   IntField{NameVal: "myname", DestNameVal: "myname"},
		},
		{
			name:  "int-min",
			input: "myname__Int_7",
			exp:   IntField{NameVal: "myname", DestNameVal: "myname", Min: &seven},
		},
		{
			name:  "int-min-max",
			input: "myname__Int_7_7",
			exp:   IntField{NameVal: "myname", DestNameVal: "myname", Min: &seven, Max: &seven},
		},
		{
			name:  "int-min-max-foreign",
			input: "myname__Int_7_7_findex",
			exp:   IntField{NameVal: "myname", DestNameVal: "myname", Min: &seven, Max: &seven, ForeignIndex: "findex"},
		},
		{
			name:   "int-min-max-extra",
			input:  "myname__Int_7_7_z_",
			exp:    IntField{NameVal: "myname", DestNameVal: "myname", Min: &seven, Max: &seven, ForeignIndex: "z"},
			expLog: "ignoring extra arguments to IntField",
		},
		{
			name:   "int-min-parseerr",
			input:  "myname__Int_7_8.9",
			expErr: "parsing max for",
		},
		{
			name:   "int-parserr-max",
			input:  "myname__Int_blah_7",
			expErr: "parsing min for",
		},
		{
			name:  "decimal-scale",
			input: "myname__Decimal_7",
			exp:   DecimalField{NameVal: "myname", DestNameVal: "myname", Scale: 7},
		},
		{
			name:  "decimal",
			input: "myname__Decimal",
			exp:   DecimalField{NameVal: "myname", DestNameVal: "myname"},
		},
		{
			name:   "decimal-err",
			input:  "myname__Decimal_!",
			expErr: "parsing scale for",
		},
		{
			name:  "stringarray-quantum",
			input: "myname__StringArray_YMD",
			exp:   StringArrayField{NameVal: "myname", DestNameVal: "myname", Quantum: "YMD"},
		},
		{
			name:  "stringarray-ttl",
			input: "myname__StringArray_YMD_30s",
			exp:   StringArrayField{NameVal: "myname", DestNameVal: "myname", Quantum: "YMD", TTL: "30s"},
		},
		{
			name:   "stringarray-extra_arg",
			input:  "myname__StringArray_YMD_30s_Z",
			exp:    StringArrayField{NameVal: "myname", DestNameVal: "myname", Quantum: "YMD", TTL: "30s"},
			expLog: "ignoring extra arguments to StringArrayField",
		},
		{
			name:  "idarray-quantum",
			input: "myname__IDArray_YMD",
			exp:   IDArrayField{NameVal: "myname", DestNameVal: "myname", Quantum: "YMD"},
		},
		{
			name:  "idarray-ttl",
			input: "myname__IDArray_YMD_30s",
			exp:   IDArrayField{NameVal: "myname", DestNameVal: "myname", Quantum: "YMD", TTL: "30s"},
		},
		{
			name:   "idarray-extra_arg",
			input:  "myname__IDArray_YMD_30s_Z",
			exp:    IDArrayField{NameVal: "myname", DestNameVal: "myname", Quantum: "YMD", TTL: "30s"},
			expLog: "ignoring extra arguments to IDArrayField",
		},
		{
			name:  "date-int",
			input: "myname__DateInt",
			exp: DateIntField{
				NameVal:     "myname",
				DestNameVal: "myname",
				Layout:      time.RFC3339,
			},
		},
		{
			name:  "date-int-layout-epoch",
			input: "myname__DateInt_2006-01-02T15:04:05Z07:00_2018-03-04T15:04:05Z",
			exp: DateIntField{
				NameVal:     "myname",
				DestNameVal: "myname",
				Layout:      time.RFC3339,
				Epoch:       time.Date(2018, time.March, 4, 15, 4, 5, 0, time.UTC),
			},
		},
		{
			name:  "date-int-layout-epoch-unit",
			input: "myname__DateInt_2006-01-02T15:04:05Z07:00_2018-03-04T15:04:05Z_D",
			exp: DateIntField{
				NameVal: "myname", DestNameVal: "myname",
				Layout: time.RFC3339,
				Epoch:  time.Date(2018, time.March, 4, 15, 4, 5, 0, time.UTC),
				Unit:   Day,
			},
		},
		{
			name:  "date-int-layout-epoch-unit-custom",
			input: "myname__DateInt_2006-01-02T15:04:05Z07:00_2018-03-04T15:04:05Z_C_10h",
			exp: DateIntField{
				NameVal: "myname", DestNameVal: "myname",
				Layout:     time.RFC3339,
				Epoch:      time.Date(2018, time.March, 4, 15, 4, 5, 0, time.UTC),
				Unit:       Custom,
				CustomUnit: "10h",
			},
		},
		{
			name:  "date-int-layout-epoch-unit-custom-2",
			input: "myname__DateInt_2006-01-02_2018-03-04_D_0",
			exp: DateIntField{
				NameVal: "myname", DestNameVal: "myname",
				Layout: "2006-01-02",
				Epoch:  time.Date(2018, time.March, 4, 0, 0, 0, 0, time.UTC),
				Unit:   Day,
			},
		},
		{
			name:   "date-int-layout-epoch-unit-custom-error",
			input:  "myname__DateInt_2006-01-02_2018-03d-04_D_0",
			expErr: `cannot parse "d-04"`,
		},
		{
			name:   "date-int-layout-epoch-unit-custom-layout-badunit",
			input:  "myname__DateInt_2006-01-02_2018-03-04_Ze_0",
			expErr: fmt.Sprintf(ErrFmtUnknownUnit, "ze"),
		},
		{
			name:   "date-int-layout-epoch-unit-custom-layout-customuniterror",
			input:  "myname__DateInt_2006-01-02_2018-03-04_C_127z",
			expErr: `parsing custom unit 127z: time: unknown unit "z" in duration "127z"`,
		},
		{
			name:  "recordtime",
			input: "myname__RecordTime",
			exp:   RecordTimeField{NameVal: "myname", Layout: "2006-01-02T15:04:05Z07:00", DestNameVal: "myname"},
		},
		{
			name:  "recordtime",
			input: "__RecordTime",
			exp:   RecordTimeField{Layout: "2006-01-02T15:04:05Z07:00"},
		},
		{
			name:  "recordtime",
			input: "myname__RecordTime_2006-01-02",
			exp:   RecordTimeField{NameVal: "myname", DestNameVal: "myname", Layout: "2006-01-02"},
		},
		{
			name:   "recordtime-layout-epoch-unit-custom-error",
			input:  "myname__RecordTime_2006-01-02_2018-03d-04_D_0",
			expErr: `cannot parse "d-04"`,
		},
		{
			name:   "recordtime-layout-epoch-unit-custom-layout-badunit",
			input:  "myname__RecordTime_2006-01-02_2018-03-04_Ze_0",
			expErr: "unknown unit",
		},
		{
			name:  "multidunder",
			input: "multi__dunder__String",
			exp:   StringField{NameVal: "multi__dunder", DestNameVal: "multi__dunder"},
		},
		{
			name:   "multi-dunder-no-fieldspec",
			input:  "multi__dunder__funder",
			expErr: "unknown field 'funder' for",
		},
		{
			name:  "map-incompatible-name",
			input: "@rbitrary.name*string!___pilosa-name-string__String",
			exp: StringField{
				NameVal:     "@rbitrary.name*string!",
				DestNameVal: "pilosa-name-string",
			},
		},
		{
			name:  "map-compatible-name",
			input: "dunderful__name__string___pilosa-name-string__String",
			exp: StringField{
				NameVal:     "dunderful__name__string",
				DestNameVal: "pilosa-name-string",
			},
		},
		{
			name:  "map-dunderful-name",
			input: "dunderful__name__string___pilosa__name__string__String",
			exp: StringField{
				NameVal:     "dunderful__name__string",
				DestNameVal: "pilosa__name__string",
			},
		},
		{
			name:  "timestamp",
			input: "purchasedate__Timestamp_ms",
			exp: TimestampField{
				NameVal:     "purchasedate",
				DestNameVal: "purchasedate",
				Granularity: "ms",
				Layout:      time.RFC3339Nano,
			},
		},
		{
			name:  "timestamp-epoch",
			input: "purchasedate__Timestamp_s_2006-01-02T15:04:05Z07:00_2018-03-04T15:04:05Z_ms",
			exp: TimestampField{
				NameVal:     "purchasedate",
				DestNameVal: "purchasedate",
				Granularity: "s",
				Layout:      time.RFC3339,
				Epoch:       time.Date(2018, time.March, 4, 15, 4, 5, 0, time.UTC),
				Unit:        Millisecond,
			},
		},
		{
			name:  "lookup-text",
			input: "a__LookupText",
			exp:   LookupTextField{NameVal: "a", DestNameVal: "a"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rl := &recordingLogger{}
			field, err := HeaderToField(test.input, rl)
			if test.expErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.expErr) {
					t.Fatalf("unexpected error exp/got:\n%s\n%v", test.expErr, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			errorIfNotEqual(t, test.exp, field)
			if test.expLog == "" && len(rl.logs) > 0 {
				t.Errorf("unexpected logs: %+v", rl.logs)
			} else if test.expLog != "" && len(rl.logs) == 0 {
				t.Errorf("expected log: '%s'", test.expLog)
			} else if test.expLog != "" {
				if !strings.Contains(rl.logs[0], test.expLog) {
					t.Errorf("exp/got:\n%s\n%s", test.expLog, rl.logs[0])
				}
			}
		})
	}
}

func errorIfNotEqual(t *testing.T, exp, got interface{}) {
	t.Helper()
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unequal exp/got:\n%+v of %[1]T\n%+v of %[2]T", exp, got)
	}
}

func errorIfNotErrMatch(t *testing.T, expErrSubstring string, got error) {
	t.Helper()
	if expErrSubstring == "" && got != nil {
		t.Fatalf("unexpected error: %v", got)
	} else if expErrSubstring == "" {
		return
	} else if expErrSubstring != "" && got == nil {
		t.Fatalf("expected error like '%s'", expErrSubstring)
	} else if !strings.Contains(got.Error(), expErrSubstring) {
		t.Fatalf("unexpected error exp/got:\n%s\n%v", expErrSubstring, got)
	}

}

type recordingLogger struct {
	logs   []string
	debugs []string
}

func (r *recordingLogger) Panicf(format string, v ...interface{}) {
	r.logs = append(r.logs, fmt.Sprintf("PANIC:"+format, v...))
	panic(r.logs)
}
func (r *recordingLogger) Errorf(format string, v ...interface{}) {
	r.Printf("ERROR:"+format, v)
}
func (r *recordingLogger) Infof(format string, v ...interface{}) {
	r.Printf("INFO:"+format, v)
}
func (r *recordingLogger) Warnf(format string, v ...interface{}) {
	r.Printf("WARN:"+format, v)
}
func (r *recordingLogger) Printf(format string, v ...interface{}) {
	r.logs = append(r.logs, fmt.Sprintf(format, v...))
}
func (r *recordingLogger) Debugf(format string, v ...interface{}) {
	r.debugs = append(r.debugs, fmt.Sprintf(format, v...))
}

// WithPrefix does nothing for the recording logger... just makes it
// implement the interface. Do not use.
func (r *recordingLogger) WithPrefix(prefix string) logger.Logger {
	panic("unimplemented")
}
