// Package rbf implements the roaring b-tree file format.
package rbf_test

/*
func itohex(v int) string { return fmt.Sprintf("0x%x", v) }

func hexdump(b []byte) { println(hex.Dump(b)) }

// treedump recursively writes the tree representation starting from a given page to STDERR.
func treedump(tx *rbf.Tx, pgno uint32, indent string, writer io.Writer) {
	page, err := tx.readPage(pgno)
	if err != nil {
		panic(err)
	}

	if rbf.IsMetaPage(page) {
		fmt.Fprintf(writer, "META(%d)\n", pgno)
		fmt.Fprintf(writer, "└── <FREELIST>\n")
		//treedump(tx, readMetaFreelistPageNo(page), indent+"    ")

		visitor := func(pgno uint32, records []*rbf.RootRecord) {
			fmt.Fprintf(writer, "└── ROOT RECORD(%d): n=%d\n", pgno, len(records))
			for _, record := range records {
				fmt.Fprintf(writer, "└── ROOT(%q) %d\n", record.Name, record.Pgno)
				treedump(tx, record.Pgno, indent+"    ", writer)

			}
		}
		rrdump(tx, readMetaRootRecordPageNo(page), visitor)

		return
	}

	// Handle
	switch typ := readFlags(page); typ {
	case PageTypeBranch:
		fmt.Fprintf(writer, "%s BRANCH(%d) n=%d\n", fmtindent(indent), pgno, readCellN(page))

		for i, n := 0, readCellN(page); i < n; i++ {
			cell := readBranchCell(page, i)
			treedump(tx, cell.Pgno, "    "+indent, writer)
		}
	case PageTypeLeaf:
		fmt.Fprintf(writer, "%s LEAF(%d) n=%d\n", fmtindent(indent), pgno, readCellN(page))
		pagedumpi(page, fmtindent("    "+indent), writer)
	default:
		panic(err)
	}
}

func rrdump(tx *Tx, pgno uint32, v func(uint32, []*RootRecord)) {
	for pgno := readMetaRootRecordPageNo(tx.meta[:]); pgno != 0; {
		page, err := tx.readPage(pgno)
		if err != nil {
			panic(err)
		}

		// Read all records on the page.
		a, err := readRootRecords(page)
		if err != nil {
			panic(err)
		}
		v(pgno, a)
		// Read next overflow page number.
		pgno = WalkRootRecordPages(page)
	}
}

func fmtindent(s string) string {
	if s == "" {
		return ""
	}
	return s + "└──"
}

// RowValues returns a list of integer values from a row bitmap.
func RowValues(b []uint64) []uint64 {
	a := make([]uint64, 0)
	for i, v := range b {
		for j := uint(0); j < 64; j++ {
			if v&(1<<j) != 0 {
				a = append(a, (uint64(i)*64)+uint64(j))
			}
		}
	}
	return a
}

func onpanic(fn func()) {
	if r := recover(); r != nil {
		fn()
	}
}
*/
