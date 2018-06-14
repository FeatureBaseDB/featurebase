package pql

import (
	"testing"
)

func TestPEG(t *testing.T) {
	p := PQL{Buffer: `
SetBit(Union(Zitmap(row==4), Intersect(Qitmap(blah>4), Ritmap(field="http://zoo9.com=\\'hello' and \"hello\"")), Hitmap(row=ag-bee)), a="4z", b=5) Count(Union(Witmap(row=5.73, frame=.10), Range(zztop><[2, 9]))) TopN(fields=["hello", "goodbye", "zero"])`[1:]}
	p.Init()
	err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	p.Execute()

	p = PQL{Buffer: `SetRowAttrs(attr="http://zoo9.com=\\'hello' "and \"hello\"")`}
	p.Init()
	err = p.Parse()
	if err == nil {
		t.Fatalf("should have been an error because of the interior unescaped double quote")
	}

	q, err := ParseString("TopN(Bitmap(id==other), field=f, n=0)")
	if err != nil {
		t.Fatalf("should have parsed: %v", err)
	}
	if q.String() != `TopN(Bitmap(id == "other"), field="f", n=0)` {
		t.Fatalf("Failed, got: %s", q)
	}

	q, err = ParseString("C(a=falsen0)")
	if err != nil {
		t.Fatalf("falsen0 should have been parsed as a string")
	}

	q, err = ParseString("Bitmap(row=4, did==other)")
	if err != nil {
		t.Fatalf("should have parsed: %v", err)
	}

	if q.String() != `Bitmap(did == "other", row=4)` {
		t.Fatalf("got %s", q)
	}

}
