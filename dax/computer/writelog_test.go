package computer_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/molecula/featurebase/v3/dax/computer"
	"github.com/stretchr/testify/assert"
)

func TestComputer_WriteLog(t *testing.T) {
	t.Run("Marshal", func(t *testing.T) {
		tableName := "tbl"

		type invalidType struct{}

		tests := []struct {
			msg            computer.LogMessage
			encodeType     string
			expVersion     byte
			expEncodeType  byte
			expMessageType byte
			expError       string
		}{
			{
				msg: &computer.ImportRoaringMessage{
					Table: tableName,
				},
				encodeType:     computer.EncodeTypeJSON,
				expVersion:     1,
				expEncodeType:  0,
				expMessageType: 0,
			},
			{
				msg: &computer.ImportMessage{
					Table: tableName,
				},
				encodeType:     computer.EncodeTypeJSON,
				expVersion:     1,
				expEncodeType:  0,
				expMessageType: 1,
			},
			{
				msg: &computer.ImportValueMessage{
					Table: tableName,
				},
				encodeType:     computer.EncodeTypeJSON,
				expVersion:     1,
				expEncodeType:  0,
				expMessageType: 2,
			},
			{
				msg: &computer.ImportRoaringShardMessage{
					Table: tableName,
				},
				encodeType:     computer.EncodeTypeJSON,
				expVersion:     1,
				expEncodeType:  0,
				expMessageType: 3,
			},

			// Error cases.
			{
				msg:        &invalidType{},
				encodeType: computer.EncodeTypeJSON,
				expError:   "don't have type",
			},
			{
				msg: &computer.ImportMessage{
					Table: tableName,
				},
				encodeType: "badencoding",
				expError:   "invalid encode type",
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				b, err := computer.MarshalLogMessage(test.msg, test.encodeType)
				if test.expError != "" {
					if assert.Error(t, err) {
						assert.Contains(t, err.Error(), test.expError)
					}
					return
				} else {
					assert.NoError(t, err)
				}

				// Version
				assert.Equal(t, test.expVersion, b[0])

				// EncodeType
				assert.Equal(t, test.expEncodeType, b[1])

				// MessageType
				assert.Equal(t, test.expMessageType, b[2])

				logMessage, err := computer.UnmarshalLogMessage(b)
				assert.NoError(t, err)

				// Get the value in Table for the instance of LogMessage.
				val := reflect.ValueOf(logMessage).Elem()
				fld := val.FieldByName("Table")

				assert.Equal(t, tableName, fld.String())
			})
		}
	})

	t.Run("Unmarshal", func(t *testing.T) {
		tests := []struct {
			b        []byte
			expError string
		}{
			{
				b:        []byte{1, 0, 1, '{', '}'},
				expError: "",
			},
			{
				b:        []byte{},
				expError: "log record does not contain a full header",
			},
			{
				b:        []byte{1, 0, 1},
				expError: "unexpected end of JSON input",
			},
			{
				b:        []byte{1, 255, 1},
				expError: "getting encoder by key: invalid encode type: 255",
			},
			{
				b:        []byte{1, 0, 255},
				expError: "unknown message type",
			},
			{
				b:        []byte{255, 0, 1},
				expError: "encode version is unsupported",
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				_, err := computer.UnmarshalLogMessage(test.b)
				if test.expError != "" {
					if assert.Error(t, err) {
						assert.Contains(t, err.Error(), test.expError)
					}
					return
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})
}
