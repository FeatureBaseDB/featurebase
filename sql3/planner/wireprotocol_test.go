package planner

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/wireprotocol"
	"github.com/stretchr/testify/assert"
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
			if tt.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
			assert.EqualValues(t, tt.want, got)
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
				m, err := newMessageError(nil, makeError())
				assert.Nil(t, err)
				return m
			},
			want: wireprotocol.TOKEN_ERROR_MESSAGE,
		},
		{
			name: "token planOp",
			message: func() wireProtocolMessage {
				m, err := newMessagePlanOp(nil, makePlanOp())
				assert.Nil(t, err)

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
				assert.Nil(t, err)
				rdr := bytes.NewReader(b)
				_, err = wireprotocol.ExpectToken(rdr, wireprotocol.TOKEN_SCHEMA_INFO)
				assert.Nil(t, err)
				m, err := newMessageSchemaInfo(nil, rdr)
				assert.Nil(t, err)
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
				assert.Nil(t, err)
				m, err := newMessageRow(nil, bytes.NewReader(b), schema)
				assert.Nil(t, err)
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
			got := message.Token()
			assert.Equal(t, tt.want, got)
		})
	}
}
