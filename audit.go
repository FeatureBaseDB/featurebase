// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"github.com/featurebasedb/featurebase/v3/testhook"
)

var NewAuditor func() testhook.Auditor = NewNopAuditor

func NewNopAuditor() testhook.Auditor {
	return testhook.NewNopAuditor()
}
