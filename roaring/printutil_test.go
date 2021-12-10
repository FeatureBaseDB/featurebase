package roaring

import (
	"fmt"
	"testing"

	"github.com/molecula/featurebase/v2/shardwidth"
)

func TestAsContainerMatrixString(t *testing.T) {
	b := NewBitmap(0)
	obs := b.AsContainerMatrixString()
	exp20 := `
            0      1      2      3      4      5      6      7      8      9      10     11     12     13     14     15     
[row 00000] 1      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      
`
	exp22 := `
            0      1      2      3      4      5      6      7      8      9      10     11     12     13     14     15     16     17     18     19     20     21     22     23     24     25     26     27     28     29     30     31     32     33     34     35     36     37     38     39     40     41     42     43     44     45     46     47     48     49     50     51     52     53     54     55     56     57     58     59     60     61     62     63     
[row 00000] 1      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      
`
	exp := exp20
	if shardwidth.Exponent == 22 {
		exp = exp22
	}
	if obs != exp {
		panic(fmt.Sprintf("unexpected output: obs='%v', exp='%v'", obs, exp))
	}
}

func TestAsContainerMatrixString2(t *testing.T) {
	b := NewBitmap(1<<16, 4<<16, 18<<16)
	obs := b.AsContainerMatrixString()
	exp20 := `
            0      1      2      3      4      5      6      7      8      9      10     11     12     13     14     15     
[row 00000] 0      1      0      0      1      0      0      0      0      0      0      0      0      0      0      0      
[row 00001] 0      0      1      0      0      0      0      0      0      0      0      0      0      0      0      0      
`

	exp22 := `
            0      1      2      3      4      5      6      7      8      9      10     11     12     13     14     15     16     17     18     19     20     21     22     23     24     25     26     27     28     29     30     31     32     33     34     35     36     37     38     39     40     41     42     43     44     45     46     47     48     49     50     51     52     53     54     55     56     57     58     59     60     61     62     63     
[row 00000] 0      1      0      0      1      0      0      0      0      0      0      0      0      0      0      0      0      0      1      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      
`

	exp := exp20
	if shardwidth.Exponent == 22 {
		exp = exp22
	}

	if obs != exp {
		panic(fmt.Sprintf("unexpected output: obs='%v', exp='%v'", obs, exp))
	}
}

func TestAsContainerMatrixString3(t *testing.T) {
	b := NewBitmap()
	for i := uint64(0); i < 1<<16; i++ {
		_, _ = b.AddN(i)
	}
	obs := b.AsContainerMatrixString()
	exp20 := `
            0      1      2      3      4      5      6      7      8      9      10     11     12     13     14     15     
[row 00000] 65536  0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      
`

	exp22 := `
            0      1      2      3      4      5      6      7      8      9      10     11     12     13     14     15     16     17     18     19     20     21     22     23     24     25     26     27     28     29     30     31     32     33     34     35     36     37     38     39     40     41     42     43     44     45     46     47     48     49     50     51     52     53     54     55     56     57     58     59     60     61     62     63     
[row 00000] 65536  0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      0      
`

	exp := exp20
	if shardwidth.Exponent == 22 {
		exp = exp22
	}

	if obs != exp {
		panic(fmt.Sprintf("unexpected output: obs='%v', exp='%v'", obs, exp))
	}
}
