package pql

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"testing"
)

type testAddDecimalCase struct {
	a   Decimal
	b   Decimal
	exp Decimal
}

func TestAddDecimal(t *testing.T) {
	toDecimal := func(a interface{}) Decimal {
		switch ac := a.(type) {
		case string:
			return mustParse(t, ac)
		case Decimal:
			return ac
		default:
			t.Fatalf("cannot support type %T", ac)
		}
		return Decimal{}
	}

	newTestAddDecimalCase := func(a, b, exp interface{}) testAddDecimalCase {
		return testAddDecimalCase{
			a:   toDecimal(a),
			b:   toDecimal(b),
			exp: toDecimal(exp),
		}
	}

	tests := []testAddDecimalCase{
		newTestAddDecimalCase("40.37", "40.37", "80.74"),
		newTestAddDecimalCase("18.5", "9.25", "27.75"),
		newTestAddDecimalCase("18.50", "9.25", "27.75"),
		newTestAddDecimalCase("-18.50", "-9.25", "-27.75"),
		newTestAddDecimalCase("-40.37", "40.37", NewDecimal(0, 0)),
		newTestAddDecimalCase("-50.38", "-50.38", "-100.76"),
		newTestAddDecimalCase("-40.381", "-40.38", "-80.761"),
		newTestAddDecimalCase("-40.3700000000000000000001", "-50.38", "-90.7500000000000000000001"),
		newTestAddDecimalCase("10.37000000000000001", "-10", "0.37000000000000001"),
		// this is a weird one. because we reduce precision when we parse strings, we
		// expect the value to be smaller than it really should be if you did
		// the math yourself.
		//
		// there's not really a good way around this unless we use math/big.Int to represent
		// our Value/Scale.
		//
		// but if we did this, to quote seebs: "i suspect that
		// our performance would tank so badly by the time we were doing >63-bit
		// numbers that there's no real-world benefit to us."
		newTestAddDecimalCase("10.370000000000000001", "-10", "0.37"),
		newTestAddDecimalCase("-9223372036854775807", "-9223372036854775807", "-18446744073709551614"),
		newTestAddDecimalCase("9223372036854775807", "9223372036854775807", "18446744073709551614"),
		newTestAddDecimalCase(NewDecimal(0, 0), NewDecimal(1, 4), NewDecimal(1, 4)),
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Logf("first %#v %#v", test.a, test.b)
			if got := AddDecimal(test.a, test.b); !got.EqualTo(test.exp) {
				t.Logf("LOG: %#v + %#v, expected %#v, got %#v", test.a, test.b, test.exp, got)
				t.Errorf("%v + %v, expected %v, got %v", test.a, test.b, test.exp, got)
			}

			t.Logf("second %v %v", test.a, test.b)
			if got := AddDecimal(test.b, test.a); !got.EqualTo(test.exp) {
				t.Logf("LOG: %#v + %#v, expected %#v, got %#v", test.a, test.b, test.exp, got)
				t.Errorf("%v + %v, expected %v, got %v", test.b, test.a, test.exp, got)
			}
		})
	}
}

func mustParse(t *testing.T, num string) Decimal {
	t.Helper()
	d, err := ParseDecimal(num)
	if err != nil {
		v := big.NewInt(0)
		if _, ok := v.SetString(num, 10); !ok {
			t.Fatalf("unexpected error parsing %s to Decimal: %v", num, err)
		}
		d.value = *v
	}
	return d
}

func TestGobEncodeDecode(t *testing.T) {
	for i := int64(-10); i < 10; i++ {
		for j := int64(-10); j < 10; j++ {
			decimal := &Decimal{value: *(big.NewInt(i)), Scale: j}
			t.Run(fmt.Sprintf("v%ds%d", i, j), func(t *testing.T) {
				encoded, err := decimal.GobEncode()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				decoded := &Decimal{}
				err = decoded.GobDecode(encoded)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !reflect.DeepEqual(decimal, decoded) {
					t.Errorf("%v != %v", decimal, encoded)
				}
			})
		}
	}

	i := int64(math.MaxInt64)
	j := int64(math.MinInt64)
	decimal := &Decimal{value: *(big.NewInt(i)), Scale: j}
	t.Run(fmt.Sprintf("v%ds%d", i, j), func(t *testing.T) {
		encoded, err := decimal.GobEncode()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		decoded := &Decimal{}
		err = decoded.GobDecode(encoded)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(decimal, decoded) {
			t.Errorf("%v != %v", decimal, encoded)
		}
	})

}
