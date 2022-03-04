// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"github.com/molecula/featurebase/v3/testhook"
)

var NewAuditor func() testhook.Auditor = NewNopAuditor

func NewNopAuditor() testhook.Auditor {
	return testhook.NewNopAuditor()
}
