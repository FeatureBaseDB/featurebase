package defs

var topTests = TableTest{
	name: "top-tests",
	Table: tbl(
		"skills",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("bools", fldTypeStringSet),
			srcHdr("bools-exist", fldTypeStringSet),
			srcHdr("id1", fldTypeID),
			srcHdr("skills", fldTypeStringSet),
			srcHdr("titles", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(1), nil, []string{"available_for_hire"}, int64(288), []string{"Marketing Manager"}, []string{"OEM negotiations", "Alumni Relations"}),
			srcRow(int64(2), nil, []string{"available_for_hire"}, int64(288), []string{"Software Engineer I"}, []string{"Chief Cook", "Bottle Washer"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select  top(1) * from skills where setcontains(skills, 'Marketing Manager');",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("bools", fldTypeStringSet),
				hdr("bools-exist", fldTypeStringSet),
				hdr("id1", fldTypeID),
				hdr("skills", fldTypeStringSet),
				hdr("titles", fldTypeStringSet),
			),
			ExpRows: rows(
				row(int64(1), nil, []string{"available_for_hire"}, int64(288), []string{"Marketing Manager"}, []string{"Alumni Relations", "OEM negotiations"}),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			// TODO(pok) this is a bit weird type consistency wise - grouping by a set in FB is grouping by the members of a set
			// 		suggest making the below query a semantic error 'can't group by set column' and
			//		force syntax like '...group by members(skills)' to make it explicit what it is doing
			//		if we want to group by a set column, it should group by distinct member combinations instead
			SQLs: sqls(
				"select top(10) count(*), skills from skills group by skills;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("skills", fldTypeStringSet),
			),
			ExpRows: rows(
				row(int64(1), string("Marketing Manager")),
				row(int64(1), string("Software Engineer I")),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			SQLs: sqls(
				"select top(1) count(*) from skills;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
	},
}
