package writelogger_test

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax/writelogger"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/assert"
)

func TestWritelogger(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "testWritelogger-*")
	assert.NoError(t, err)

	// Remove the temp directory.
	defer func() {
		os.RemoveAll(tmpDir)
	}()

	t.Run("Basic", func(t *testing.T) {
		type payload struct {
			Foo string `json:"foo"`
			Bar int    `json:"bar"`
		}

		wl := writelogger.New(tmpDir, logger.NopLogger)

		table := "tbl"
		partition := 1
		version := 0
		key := "keys"

		msg1 := payload{
			Foo: "message 1",
			Bar: 88,
		}

		// Write the message.
		msg, err := json.Marshal(msg1)
		assert.NoError(t, err)

		err = wl.AppendMessage(bucket(table, partition), key, version, msg)
		assert.NoError(t, err)

		// Read the message.
		readcloser, err := wl.LogReader(bucket(table, partition), key, version)
		assert.NoError(t, err)
		defer readcloser.Close()

		buf, err := io.ReadAll(readcloser)
		assert.NoError(t, err)

		var out payload

		err = json.Unmarshal(buf, &out)
		assert.NoError(t, err)

		assert.Equal(t, msg1.Foo, out.Foo)
		assert.Equal(t, msg1.Bar, out.Bar)
	})
}

func bucket(table string, partition int) string {
	return path.Join(table, fmt.Sprintf("%d", partition))

}
