package sql

type router struct {
	direct  map[QueryMask]handler
	filters []maskFilter
}

type maskFilter struct {
	optional QueryMask
	required []QueryMask
	handler  handler
}

func newRouter() *router {
	selectRouter := &router{
		direct: make(map[QueryMask]handler),
	}

	selectRouter.addFilter(
		NewQueryMask(
			SelectPartID|SelectPartStar|SelectPartField|SelectPartFields,
			FromPartTable,
			WherePartFieldCondition|WherePartMultiFieldCondition|WherePartIDCondition,
			0,
			0,
		),
		[]QueryMask{},
		handlerSelectFieldsFromTableWhere{},
	)
	////
	selectRouter.addRoute("select distinct fld from tbl", handlerSelectDistinctFromTable{})
	////
	selectRouter.addFilter(
		NewQueryMask(
			SelectPartCountStar|SelectPartCountField|SelectPartCountDistinctField,
			FromPartTable,
			WherePartFieldCondition|WherePartMultiFieldCondition|WherePartIDCondition,
			0,
			0,
		),
		[]QueryMask{},
		handlerSelectCountFromTableWhere{},
	)
	////
	selectRouter.addFilter(
		NewQueryMask(
			SelectPartMinField|SelectPartMaxField|SelectPartSumField|SelectPartAvgField,
			FromPartTable,
			WherePartFieldCondition|WherePartMultiFieldCondition|WherePartIDCondition,
			0,
			0,
		),
		[]QueryMask{},
		handlerSelectFuncFromTableWhere{},
	)
	////
	groupByOptional := NewQueryMask(
		SelectPartField|SelectPartFields|SelectPartCountStar|SelectPartSumField,
		FromPartTable,
		WherePartFieldCondition, // TODO: this can probably handle fields as well
		GroupByPartField|GroupByPartFields,
		HavingPartCondition,
	)
	selectRouter.addFilter(
		groupByOptional,
		[]QueryMask{NewQueryMask(0, 0, 0, GroupByPartField, 0)},
		handlerSelectGroupBy{},
	)
	selectRouter.addFilter(
		groupByOptional,
		[]QueryMask{NewQueryMask(0, 0, 0, GroupByPartFields, 0)},
		handlerSelectGroupBy{},
	)
	selectRouter.addRoute("select fld, count(fld) from tbl group by fld", handlerSelectGroupBy{})
	selectRouter.addRoute("select fld1, count(fld1) from tbl where fld2=1 group by fld1", handlerSelectGroupBy{})

	selectRouter.addRoute("select count(*) from tbl1 INNER JOIN tbl2 ON tbl1._id = tbl2.bsi", handlerSelectJoin{})
	selectRouter.addRoute("select count(*) from tbl1 INNER JOIN tbl2 ON tbl1._id = tbl2.bsi where fld1=1", handlerSelectJoin{})
	selectRouter.addRoute("select count(*) from tbl1 INNER JOIN tbl2 ON tbl1._id = tbl2.bsi where fld1=1 and fld2=2", handlerSelectJoin{})
	selectRouter.addRoute("select _id from tbl1 INNER JOIN tbl2 ON tbl1._id = tbl2.bsi", handlerSelectJoin{})
	selectRouter.addRoute("select _id from tbl1 INNER JOIN tbl2 ON tbl1._id = tbl2.bsi where fld1=1", handlerSelectJoin{})
	selectRouter.addRoute("select _id from tbl1 INNER JOIN tbl2 ON tbl1._id = tbl2.bsi where fld1=1 and fld2=2", handlerSelectJoin{})

	return selectRouter
}

func (r *router) addRoute(sql string, handler handler) {
	r.direct[MustGenerateMask(sql)] = handler
}

func (r *router) addFilter(opt QueryMask, req []QueryMask, handler handler) {
	mf := maskFilter{
		optional: opt,
		required: req,
		handler:  handler,
	}
	r.filters = append(r.filters, mf)
}

func (r *router) handler(qm QueryMask) handler {
	// First, check for a direct mapping.
	// Zero out the orderBy and limit mask, because
	// those are not specific to the query processing.
	zm := QueryMask{
		SelectMask:  qm.SelectMask,
		FromMask:    qm.FromMask,
		WhereMask:   qm.WhereMask,
		GroupByMask: qm.GroupByMask,
		HavingMask:  qm.HavingMask,
	}
	if h, ok := r.direct[zm]; ok {
		return h
	}
	for _, mf := range r.filters {
		if applyMaskFilter(&qm, mf) {
			return mf.handler
		}
	}
	return nil
}

// applyMaskFilter returns true if m passes the filter mf.
// Note: only certain query parts are included; namely,
// the orderBy and limit masks are not applied to the
// filter. A mask can satisfy any part of the optional
// filter to pass through, but it MUST satisfy all parts
// of the required filter.
func applyMaskFilter(m *QueryMask, mf maskFilter) bool {
	if !m.ApplyFilter(mf.optional) {
		return false
	}
	for _, req := range mf.required {
		if m.SelectMask&req.SelectMask != req.SelectMask {
			return false
		}
		if m.FromMask&req.FromMask != req.FromMask {
			return false
		}
		if m.WhereMask&req.WhereMask != req.WhereMask {
			return false
		}
		if m.GroupByMask&req.GroupByMask != req.GroupByMask {
			return false
		}
		if m.HavingMask&req.HavingMask != req.HavingMask {
			return false
		}
	}
	return true
}
