package planner

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/wireprotocol"
)

func Test_newMessageError(t *testing.T) {
	type args struct {
		p      *ExecutionPlanner
		reader io.Reader
	}
	tests := []struct {
		name    string
		args    args
		want    *messageError
		wantErr bool
	}{
		{
			name:    "err read len",
			args:    args{p: nil, reader: bytes.NewReader([]byte{})},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "err read name",
			args:    args{p: nil, reader: bytes.NewReader([]byte{0x0, 0x0, 0x0, 0x1})},
			want:    nil,
			wantErr: true,
		},
		{
			name: "happy path",
			args: args{p: nil, reader: bytes.NewReader([]byte{0x0, 0x0, 0x0, 0x3, 0x41, 0x42, 0x43})},
			want: &messageError{
				token: wireprotocol.TOKEN_ERROR_MESSAGE,
				err:   errors.New("ABC"),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newMessageError(tt.args.p, tt.args.reader)
			if (err != nil) != tt.wantErr {
				t.Errorf("newMessageError() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newMessageError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_Token(t *testing.T) {
	makeError := func() io.Reader {
		// errorMessage "ABC"
		return bytes.NewReader([]byte{0x0, 0x0, 0x0, 0x3, 0x41, 0x42, 0x43})
	}
	makePlanOp := func() io.Reader {
		var buf bytes.Buffer
		name := fbClusterInfo
		binary.Write(&buf, binary.BigEndian, (int32)(len(name)))
		buf.WriteString(name)
		b := buf.Bytes()
		fmt.Printf("%#v", b)
		return bytes.NewReader(buf.Bytes())
	}
	tests := []struct {
		name    string
		message func() wireProtocolMessage
		want    int16
	}{
		{
			name: "token error",
			message: func() wireProtocolMessage {
				m, _ := newMessageError(nil, makeError())
				return m
			},
			want: wireprotocol.TOKEN_ERROR_MESSAGE,
		},
		{
			name: "token planOp",
			message: func() wireProtocolMessage {
				m, err := newMessagePlanOp(nil, makePlanOp())
				if err != nil {
					t.Fatal(err)
				}

				return m
			},
			want: wireprotocol.TOKEN_PLAN_OP,
		},
		{
			name: "token schemaInfo",
			message: func() wireProtocolMessage {
				s := types.Schema{
					&types.PlannerColumn{
						ColumnName: "col1",
						Type:       parser.NewDataTypeID(),
					},
				}

				b, err := wireprotocol.WriteSchema(s)
				if err != nil {
					t.Fatal(err)
				}
				rdr := bytes.NewReader(b)
				_, err = wireprotocol.ExpectToken(rdr, wireprotocol.TOKEN_SCHEMA_INFO)
				if err != nil {
					t.Fatal(err)
				}
				m, err := newMessageSchemaInfo(nil, rdr)
				if err != nil {
					t.Fatal(err)
				}
				return m
			},

			want: wireprotocol.TOKEN_SCHEMA_INFO,
		},
		{
			name: "token Row",
			message: func() wireProtocolMessage {
				schema := types.Schema{
					&types.PlannerColumn{
						ColumnName: "col1",
						Type:       parser.NewDataTypeID(),
					},
				}
				row := types.Row{
					int64(1),
				}

				b, err := wireprotocol.WriteRow(row, schema)
				if err != nil {
					t.Fatal(err)
				}
				m, err := newMessageRow(nil, bytes.NewReader(b), schema)
				if err != nil {
					t.Fatal(err)
				}
				return m
			},
			want: wireprotocol.TOKEN_ROW,
		},
		{
			name: "token Done",
			message: func() wireProtocolMessage {
				m, _ := newMessageDone(nil, nil)
				return m
			},
			want: wireprotocol.TOKEN_DONE,
		},
	}
	for _, tt := range tests {
		message := tt.message()
		t.Run(tt.name, func(t *testing.T) {
			if got := message.Token(); got != tt.want {
				t.Errorf("messageError.Token() = %v, want %v", got, tt.want)
			}
		})
	}
}
