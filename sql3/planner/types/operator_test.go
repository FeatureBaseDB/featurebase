package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRow_Append(t *testing.T) {
	tests := []struct {
		name string
		src  Row
		arg  Row
		want Row
	}{
		{
			name: "basic",
			src:  Row{1, 2, 3},
			arg:  Row{4, 5, 6, 7},
			want: Row{1, 2, 3, 4, 5, 6, 7},
		},
		{
			name: "left",
			src:  Row{},
			arg:  Row{4, 5, 6},
			want: Row{4, 5, 6},
		},
		{
			name: "right",
			src:  Row{1, 2, 3},
			arg:  Row{},
			want: Row{1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.src.Append(tt.arg)
			assert.Equal(t, tt.want, got)
		})
	}
}
