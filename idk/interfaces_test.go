package idk

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDecimalFieldPilosafy(t *testing.T) {
	f := DecimalField{NameVal: "a", Scale: 3}
	if ret, err := f.PilosafyVal(float32(0.9)); err != nil || ret != int64(900) {
		t.Errorf("got: %v of %[1]T, err: %v", ret, err)
	}

}

func TestPilosafyVal(t *testing.T) {
	tests := []struct {
		field   Field
		vals    []interface{}
		exps    []interface{}
		expErrs []string
	}{
		{
			field: StringArrayField{},
			vals: []interface{}{
				"a,b,c",
				"[a,b,c]",
				""},
			exps: []interface{}{
				[]string{"a", "b", "c"},
				[]string{"a", "b", "c"},
				[]string(nil)},
		},
		{
			field: IDArrayField{},
			vals: []interface{}{
				"1,2,3",
				"[1,2,3]",
				""},
			exps: []interface{}{
				[]uint64{1, 2, 3},
				[]uint64{1, 2, 3},
				[]uint64(nil)},
		},
		{
			field: IntField{},
			vals: []interface{}{
				1,
				"1",
				"",
				nil,
			},
			exps: []interface{}{
				int64(1),
				int64(1),
				nil,
				nil,
			},
		},
		{
			field: DecimalField{Scale: 2},
			vals: []interface{}{
				1,
				"1",
				"",
				nil,
			},
			exps: []interface{}{
				int64(100),
				int64(100),
				nil,
				nil,
			},
		},
		{
			field:   IDField{},
			vals:    []interface{}{""},
			exps:    []interface{}{nil},
			expErrs: []string(nil),
		},
		{
			field:   BoolField{},
			vals:    []interface{}{""},
			exps:    []interface{}{nil},
			expErrs: []string(nil),
		},
		{
			field:   StringField{},
			vals:    []interface{}{""},
			exps:    []interface{}{""},
			expErrs: []string(nil),
		},
		{
			field:   IntField{},
			vals:    []interface{}{""},
			exps:    []interface{}{nil},
			expErrs: []string(nil),
		},
		{
			field:   DecimalField{},
			vals:    []interface{}{""},
			exps:    []interface{}{nil},
			expErrs: []string(nil),
		},
		{
			field:   StringArrayField{},
			vals:    []interface{}{""},
			exps:    []interface{}{[]string(nil)},
			expErrs: []string(nil),
		},
		{
			field:   IDArrayField{},
			vals:    []interface{}{""},
			exps:    []interface{}{[]uint64(nil)},
			expErrs: []string(nil),
		},
		{
			field:   DateIntField{},
			vals:    []interface{}{""},
			exps:    []interface{}{nil},
			expErrs: []string(nil),
		},
		{
			field:   RecordTimeField{},
			vals:    []interface{}{""},
			exps:    []interface{}{nil},
			expErrs: []string(nil),
		},
		{
			field:   TimestampField{},
			vals:    []interface{}{""},
			exps:    []interface{}{nil},
			expErrs: []string(nil),
		},
		{
			field:   SignedIntBoolKeyField{},
			vals:    []interface{}{"", int64(-2), int(2), uint64(3)},
			exps:    []interface{}{nil, int64(-2), int64(2), int64(3)},
			expErrs: []string(nil),
		},
		{
			field:   IgnoreField{},
			vals:    []interface{}{""},
			exps:    []interface{}{nil},
			expErrs: []string(nil),
		},
	}

	for i, test := range tests {
		if len(test.exps) > 0 && len(test.vals) != len(test.exps) {
			panic(fmt.Sprintf("invalid test at %d", i))
		}
		if len(test.expErrs) > 0 && len(test.vals) != len(test.expErrs) {
			panic(fmt.Sprintf("invalid test at %d", i))
		}
		for j, val := range test.vals {
			t.Run(fmt.Sprintf("test%dval%d", i, j), func(t *testing.T) {
				pval, err := test.field.PilosafyVal(val)
				if len(test.exps) > 0 {
					errorIfNotEqual(t, test.exps[j], pval)
				}
				if len(test.expErrs) > 0 {
					errorIfNotErrMatch(t, test.expErrs[j], err)
				}
			})
		}
	}
}

func TestTTLOf(t *testing.T) {
	tests := []struct {
		name        string
		field       Field
		expectedTTL time.Duration
		expectedErr string
	}{
		{
			name:        "ttl StringField",
			field:       StringField{NameVal: "a", Mutex: false, Quantum: "YMD", TTL: "1s"},
			expectedTTL: time.Second,
			expectedErr: "",
		},
		{
			name:        "ttl IDField",
			field:       IDField{NameVal: "a", Mutex: false, Quantum: "YMD", TTL: "60s"},
			expectedTTL: time.Second * 60,
			expectedErr: "",
		},
		{
			name:        "ttl StringArrayField",
			field:       StringArrayField{NameVal: "a", Quantum: "YMD", TTL: "1h"},
			expectedTTL: time.Hour,
			expectedErr: "",
		},
		{
			name:        "ttl IDArrayField",
			field:       StringArrayField{NameVal: "a", Quantum: "YMD", TTL: "1h"},
			expectedTTL: time.Hour,
			expectedErr: "",
		},
		{
			name:        "ttl bad format",
			field:       StringField{NameVal: "a", Mutex: false, Quantum: "YMD", TTL: "0ssss"},
			expectedTTL: 0,
			expectedErr: "unable to parse TTL",
		},
		{
			name:        "ttl empty",
			field:       StringField{NameVal: "a", Mutex: false, Quantum: "YMD", TTL: ""},
			expectedTTL: 0,
			expectedErr: "",
		},
		{
			name:        "no ttl",
			field:       StringField{NameVal: "a", Mutex: false, Quantum: "YMD"},
			expectedTTL: 0,
			expectedErr: "",
		},
		{
			name:        "ttl IntField",
			field:       IntField{NameVal: "a"},
			expectedTTL: 0,
			expectedErr: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ttl, err := TTLOf(test.field)
			if ttl != test.expectedTTL {
				t.Errorf("expected TTL: '%v', got: '%v'", test.expectedTTL, ttl)
			}
			if err != nil && !strings.Contains(err.Error(), test.expectedErr) {
				t.Errorf("expected error: '%s', got: '%s'", test.expectedErr, err.Error())
			}
		})
	}

}

func TestTimestampField_validateDuration(t *testing.T) {
	type args struct {
		dur    int64
		offset int64
		unit   Unit
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		errStr  string
	}{
		{
			name: "max-max",
			args: args{
				dur:    0,
				offset: TimestampToVal(Unit("s"), MaxTimestamp),
				unit:   Unit("s"),
			},
			wantErr: false,
		},
		{
			name: "oor-max",
			args: args{
				dur:    1,
				offset: TimestampToVal(Unit("s"), MaxTimestamp),
				unit:   Unit("s"),
			},
			wantErr: true,
			errStr:  "value + epoch is too far from Unix epoch",
		},
		{
			name: "max-oor",
			args: args{
				dur:    0,
				offset: TimestampToVal(Unit("s"), MaxTimestamp.Add(1*time.Second)),
				unit:   Unit("s"),
			},
			wantErr: true,
			errStr:  "value + epoch is too far from Unix epoch",
		},
		{
			name: "min-min",
			args: args{
				dur:    0,
				offset: TimestampToVal(Unit("s"), MinTimestamp),
				unit:   Unit("s"),
			},
			wantErr: false,
		},
		{
			name: "oor-min",
			args: args{
				dur:    -1,
				offset: TimestampToVal(Unit("s"), MinTimestamp),
				unit:   Unit("s"),
			},
			wantErr: true,
			errStr:  "value + epoch is too far from Unix epoch",
		},
		{
			name: "min-oor",
			args: args{
				dur:    0,
				offset: TimestampToVal(Unit("s"), MinTimestamp.Add(-1*time.Second)),
				unit:   Unit("s"),
			},
			wantErr: true,
			errStr:  "value + epoch is too far from Unix epoch",
		},
		{
			name: "max-max",
			args: args{
				dur:    0,
				offset: TimestampToVal(Unit("ns"), MaxTimestampNano),
				unit:   Unit("ns"),
			},
			wantErr: false,
		},
		{
			name: "oor-max",
			args: args{
				dur:    1,
				offset: TimestampToVal(Unit("ns"), MaxTimestampNano),
				unit:   Unit("ns"),
			},
			wantErr: true,
			errStr:  "value + epoch is too far from Unix epoch",
		},
		{
			name: "max-oor",
			args: args{
				dur:    0,
				offset: TimestampToVal(Unit("ns"), MaxTimestampNano.Add(1*time.Nanosecond)),
				unit:   Unit("ns"),
			},
			wantErr: true,
			errStr:  "value + epoch is too far from Unix epoch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateDuration(tt.args.dur, tt.args.offset, tt.args.unit); (err != nil) != tt.wantErr {
				if !strings.Contains(err.Error(), tt.errStr) {
					t.Errorf("TimestampField.validateDuration() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}

func Test_validateTimestamp(t *testing.T) {
	type args struct {
		unit Unit
		ts   time.Time
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "nano-max",
			args: args{
				unit: Unit("ns"),
				ts:   MaxTimestampNano,
			},
			wantErr: false,
		},
		{
			name: "nano-min",
			args: args{
				unit: Unit("ns"),
				ts:   MinTimestampNano,
			},
			wantErr: false,
		},
		{
			name: "nano-max-oor",
			args: args{
				unit: Unit("ns"),
				ts:   MaxTimestampNano.Add(1 * time.Nanosecond),
			},
			wantErr: true,
		},
		{
			name: "nano-min-oor",
			args: args{
				unit: Unit("ns"),
				ts:   MinTimestampNano.Add(-1 * time.Nanosecond),
			},
			wantErr: true,
		},
		{
			name: "ms-max",
			args: args{
				unit: Unit("ms"),
				ts:   MaxTimestamp,
			},
			wantErr: false,
		},
		{
			name: "ms-min",
			args: args{
				unit: Unit("ms"),
				ts:   MinTimestamp,
			},
			wantErr: false,
		},
		{
			name: "ms-max-oor",
			args: args{
				unit: Unit("ms"),
				ts:   MaxTimestamp.Add(1 * time.Millisecond),
			},
			wantErr: true,
		},
		{
			name: "ms-min-oor",
			args: args{
				unit: Unit("ms"),
				ts:   MinTimestamp.Add(-1 * time.Millisecond),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateTimestamp(tt.args.unit, tt.args.ts); (err != nil) != tt.wantErr {
				t.Errorf("validateTimestamp() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
