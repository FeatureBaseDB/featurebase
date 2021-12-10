package pilosa

import (
	"github.com/molecula/featurebase/v2/testhook"
)

var NewAuditor func() testhook.Auditor = NewNopAuditor

func NewNopAuditor() testhook.Auditor {
	return testhook.NewNopAuditor()
}
