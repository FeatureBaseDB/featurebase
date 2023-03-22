// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

func Test_coerceValue(t *testing.T) {
	type args struct {
		sourceType parser.ExprDataType
		targetType parser.ExprDataType
		value      interface{}
		atPos      parser.Pos
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name:    "int-int",
			args:    args{sourceType: &parser.DataTypeInt{}, targetType: &parser.DataTypeInt{}, value: 1, atPos: parser.Pos{}},
			want:    1,
			wantErr: false,
		}, {
			name:    "int-id",
			args:    args{sourceType: &parser.DataTypeInt{}, targetType: &parser.DataTypeID{}, value: int64(42), atPos: parser.Pos{}},
			want:    int64(42),
			wantErr: false,
		}, {
			name:    "int-decimal",
			args:    args{sourceType: &parser.DataTypeInt{}, targetType: &parser.DataTypeDecimal{Scale: 2}, value: int64(1), atPos: parser.Pos{}},
			want:    pql.NewDecimal(int64(math.Pow(10, float64(2))), 2),
			wantErr: false,
		}, {
			name:    "int-timestamp",
			args:    args{sourceType: &parser.DataTypeInt{}, targetType: &parser.DataTypeTimestamp{}, value: int64(1679499982), atPos: parser.Pos{}},
			want:    time.Unix(1679499982, 0).UTC(),
			wantErr: false,
		}, {
			name:    "ID-int",
			args:    args{sourceType: &parser.DataTypeID{}, targetType: &parser.DataTypeInt{}, value: int64(42), atPos: parser.Pos{}},
			want:    int64(42),
			wantErr: false,
		}, {
			name:    "ID-decimal",
			args:    args{sourceType: &parser.DataTypeID{}, targetType: &parser.DataTypeDecimal{Scale: 2}, value: int64(1), atPos: parser.Pos{}},
			want:    pql.NewDecimal(int64(math.Pow(10, float64(2))), 2),
			wantErr: false,
		}, {
			name:    "ID-timestamp",
			args:    args{sourceType: &parser.DataTypeID{}, targetType: &parser.DataTypeTimestamp{}, value: int64(1679499982), atPos: parser.Pos{}},
			want:    time.Unix(1679499982, 0).UTC(),
			wantErr: false,
		}, {
			name:    "ID-ID",
			args:    args{sourceType: &parser.DataTypeID{}, targetType: &parser.DataTypeID{}, value: 0, atPos: parser.Pos{}},
			want:    0,
			wantErr: false,
		}, {
			name:    "decimal-decimal",
			args:    args{sourceType: &parser.DataTypeDecimal{}, targetType: &parser.DataTypeDecimal{}, value: 0, atPos: parser.Pos{}},
			want:    0,
			wantErr: false,
		}, {
			name:    "string-string",
			args:    args{sourceType: &parser.DataTypeString{}, targetType: &parser.DataTypeString{}, value: "hello", atPos: parser.Pos{}},
			want:    "hello",
			wantErr: false,
		}, {
			name:    "string-timestamp",
			args:    args{sourceType: &parser.DataTypeString{}, targetType: &parser.DataTypeTimestamp{}, value: "2022-03-24", atPos: parser.Pos{}},
			want:    func() time.Time { tm, _ := time.ParseInLocation("2006-01-02", "2022-03-24", time.UTC); return tm }(),
			wantErr: false,
		}, {
			name:    "timestamp-timestamp",
			args:    args{sourceType: &parser.DataTypeTimestamp{}, targetType: &parser.DataTypeTimestamp{}, value: 0, atPos: parser.Pos{}},
			want:    0,
			wantErr: false,
		}, {
			name:    "idset-idset",
			args:    args{sourceType: &parser.DataTypeIDSet{}, targetType: &parser.DataTypeIDSet{}, value: 0, atPos: parser.Pos{}},
			want:    0,
			wantErr: false,
		}, {
			name:    "idset-quantum",
			args:    args{sourceType: &parser.DataTypeIDSet{}, targetType: &parser.DataTypeIDSetQuantum{}, value: 10, atPos: parser.Pos{}},
			want:    []interface{}{nil, 10},
			wantErr: false,
		}, {
			name:    "stringset-idset",
			args:    args{sourceType: &parser.DataTypeStringSet{}, targetType: &parser.DataTypeStringSet{}, value: 0, atPos: parser.Pos{}},
			want:    0,
			wantErr: false,
		}, {
			name:    "stringset-quantum",
			args:    args{sourceType: &parser.DataTypeStringSet{}, targetType: &parser.DataTypeStringSetQuantum{}, value: "YMD", atPos: parser.Pos{}},
			want:    []interface{}{nil, "YMD"},
			wantErr: false,
		}, {
			name:    "tuple-stringsetquantum",
			args:    args{sourceType: &parser.DataTypeTuple{}, targetType: &parser.DataTypeStringSetQuantum{}, value: 0, atPos: parser.Pos{}},
			want:    0,
			wantErr: false,
		}, {
			name:    "tuple-idsetquantum",
			args:    args{sourceType: &parser.DataTypeTuple{}, targetType: &parser.DataTypeIDSetQuantum{}, value: 0, atPos: parser.Pos{}},
			want:    0,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := coerceValue(tt.args.sourceType, tt.args.targetType, tt.args.value, tt.args.atPos)
			if (err != nil) != tt.wantErr {
				t.Errorf("coerceValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("coerceValue() = %v, want %v", got, tt.want)
			}
		})
	}
}
