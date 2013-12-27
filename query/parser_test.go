package query

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	. "github.com/smartystreets/goconvey/convey"
)

func TestQueryParser(t *testing.T) {
	Convey("Basic query parse", t, func() {

		tokens := Lex("union(get(10,general), get(11,brand), get(12))")
		qp := QueryParser{}
		q, err := qp.Parse(tokens)
		if err != nil {
			panic(err)
		}
		spew.Dump(q)
	})
}
