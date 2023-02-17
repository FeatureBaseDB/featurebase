package cli

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitter(t *testing.T) {
	s := newSplitter(newNopReplacer())
	t.Run("Split", func(t *testing.T) {
		tests := []struct {
			line            string
			expQueryParts   []queryPart
			expMetaCommands []metaCommand
			expError        string
		}{
			{
				line: `foo`,
				expQueryParts: []queryPart{
					newPartRaw("foo"),
				},
			},
			{
				line: `foo;`,
				expQueryParts: []queryPart{
					newPartRaw("foo"),
					newPartTerminator(),
				},
			},
			{
				line: `foo; `,
				expQueryParts: []queryPart{
					newPartRaw("foo"),
					newPartTerminator(),
				},
			},
			{
				line: `foo; ; `,
				expQueryParts: []queryPart{
					newPartRaw("foo"),
					newPartTerminator(),
				},
			},
			{
				line: `foo; bar`,
				expQueryParts: []queryPart{
					newPartRaw("foo"),
					newPartTerminator(),
					newPartRaw("bar"),
				},
			},
			{
				line: `foo; bar;`,
				expQueryParts: []queryPart{
					newPartRaw("foo"),
					newPartTerminator(),
					newPartRaw("bar"),
					newPartTerminator(),
				},
			},
			{
				line: `\q`,
				expMetaCommands: []metaCommand{
					&metaQuit{},
				},
			},
			{
				line: ` \p`,
				expMetaCommands: []metaCommand{
					&metaPrint{},
				},
			},
			{
				line: `\q \p`,
				expMetaCommands: []metaCommand{
					&metaQuit{},
					&metaPrint{},
				},
			},
			{
				line: `\q \p arg1 arg2`,
				expMetaCommands: []metaCommand{
					&metaQuit{},
					&metaPrint{},
				},
			},
			{
				line: `\set`,
				expMetaCommands: []metaCommand{
					&metaSet{
						args: []string{},
					},
				},
			},
			{
				line: `\set arg1 arg2`,
				expMetaCommands: []metaCommand{
					&metaSet{
						args: []string{"arg1", "arg2"},
					},
				},
			},
			{
				line: `\set 'arg1' 'arg2'`,
				expMetaCommands: []metaCommand{
					&metaSet{
						args: []string{"arg1", "arg2"},
					},
				},
			},
			{
				line: `\set 'arg1' '"arg2"'`,
				expMetaCommands: []metaCommand{
					&metaSet{
						args: []string{"arg1", "\"arg2\""},
					},
				},
			},
			{
				line:     `\`,
				expError: "unsupported meta-command:",
			},
			{
				line:     `\xyzxyz`,
				expError: "unsupported meta-command:",
			},
		}
		for i, tt := range tests {
			t.Run(fmt.Sprintf("test-%d-%s", i, tt.line), func(t *testing.T) {
				qps, mcs, err := s.split(tt.line)
				if tt.expError != "" {
					if assert.Error(t, err) {
						assert.Contains(t, err.Error(), tt.expError)
					}
					return
				}

				assert.NoError(t, err)
				assert.ElementsMatch(t, tt.expQueryParts, qps)
				assert.ElementsMatch(t, tt.expMetaCommands, mcs)
			})
		}
	})
}
