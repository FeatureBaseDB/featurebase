// Copyright 2021 Molecula Corp. All rights reserved.
package rbf

import (
	"fmt"
	"strings"

	txkey "github.com/molecula/featurebase/v3/short_txkey"
	"github.com/molecula/featurebase/v3/vprint"
)

// we don't currently use dumpAllPages but it's tricky enough to get right
// that it's probably worth keeping as a debugging tool.
var _ = (*Tx).dumpAllPages

func (tx *Tx) dumpAllPages(showLeaves bool) error {

	infos, err := tx.PageInfos()
	if err != nil {
		return err
	}

	// Write header.
	fmt.Printf("Pgno     ")
	fmt.Printf("TYPE       ")
	spc := strings.Repeat(" ", 29)
	fmt.Printf("TREE                           " + spc)
	fmt.Printf("EXTRA\n")

	fmt.Printf("======== ")
	fmt.Printf("========== ")
	fmt.Printf("============================== " + spc)
	fmt.Printf("====================\n")

	// Print one line for each page.
	for pgno, info := range infos {
		switch info := info.(type) {
		case *MetaPageInfo:
			fmt.Printf("Pgno:%-8d ", pgno)
			fmt.Printf("%-10s ", "meta")
			fmt.Printf("%-54s ", "")
			fmt.Printf("pageN=%d,walid=%d,rootrec=%d,freelist=%d\n", info.PageN, info.WALID, info.RootRecordPageNo, info.FreelistPageNo)

		case *RootRecordPageInfo:
			fmt.Printf("Pgno:%-8d ", pgno)
			fmt.Printf("%-10s ", "rootrec")
			fmt.Printf("%-54s ", "")
			fmt.Printf("next=%d\n", info.Next)

			page, _, err := tx.readPage(uint32(pgno))
			vprint.PanicOn(err)
			rootRecords, err := readRootRecords(page)
			vprint.PanicOn(err)
			for k, rr := range rootRecords {
				fmt.Printf("  [%02v] Name:'%v'  pgno:%v\n", k, prefixToString(rr.Name), rr.Pgno)
			}

		case *LeafPageInfo:
			if !showLeaves {
				continue
			}
			fmt.Printf("Pgno:%-8d ", pgno)
			fmt.Printf("%-10s ", "leaf")
			fmt.Printf("%-54q ", prefixToString(info.Tree))
			fmt.Printf("flags=x%x,celln=%d\n", info.Flags, info.CellN)

			page, _, err := tx.readPage(uint32(pgno))
			vprint.PanicOn(err)

			var leafCells [PageSize / 8]leafCell
			cells := readLeafCells(page, leafCells[:])
			for k, cell := range cells {
				fmt.Printf("  [%02v] : (container)Key:%v Type:%v  BitN:%v len(Data):%v\n", k, cell.Key, cell.Type, cell.BitN, len(cell.Data))
			}

		case *BranchPageInfo:
			fmt.Printf("Pgno:%-8d ", pgno)
			fmt.Printf("%-10s ", "branch")
			fmt.Printf("%-54q ", prefixToString(info.Tree))
			fmt.Printf("flags=x%x,celln=%d\n", info.Flags, info.CellN)

			page, _, err := tx.readPage(uint32(pgno))
			vprint.PanicOn(err)

			cells := readBranchCells(page)
			for i, cell := range cells {
				fmt.Printf("  [%02v] : (ChildPages's smallest) Key:%05v  ->  (Child) pgno:%v\n", i, cell.LeftKey, cell.ChildPgno)
			}

		case *BitmapPageInfo:
			fmt.Printf("Pgno:%-8d ", pgno)
			fmt.Printf("%-10s ", "bitmap")
			fmt.Printf("%-54q ", prefixToString(info.Tree))
			fmt.Printf("-\n")

		case *FreePageInfo:
			fmt.Printf("Pgno:%-8d ", pgno)
			fmt.Printf("%-10s ", "free")
			fmt.Printf("%-54s ", "")
			fmt.Printf("-\n")

		case nil:
			fmt.Printf("Pgno:%-8d ", pgno)
			fmt.Printf("%-10s ", "<nil> problem, corrupt page set")
			fmt.Printf("%-54s ", "")
			fmt.Printf("-\n")

		default:
			panic(fmt.Sprintf("unexpected page info type %T at pgno %v", info, pgno))
		}
	}
	return nil
}

func (tx *Tx) dumpPages(pgnos []uint32) error {
	// Fetch the page.
	pages, err := tx.Pages(pgnos)
	if err != nil {
		return err
	}

	for _, page := range pages {
		switch page := page.(type) {
		case *MetaPage:
			printMetaPage(page)
		case *RootRecordPage:
			printRootRecordPage(page)
		case *LeafPage:
			printLeafPage(page)
		case *BranchPage:
			printBranchPage(page)
		case *BitmapPage:
			printBitmapPage(page)
		case *FreePage:
			printFreePage(page)
		default:
			return fmt.Errorf("unexpected page type %T", page)
		}
		fmt.Printf("\n")
	}
	return nil
}

var _ = (&Tx{}).dumpPages

func printMetaPage(page *MetaPage) {
	fmt.Printf("Pgno: %d\n", page.Pgno)
	fmt.Printf("Type: meta\n")
	fmt.Printf("PageN: %d\n", page.PageN)
	fmt.Printf("WALID: %d\n", page.WALID)
	fmt.Printf("Root Record Pgno: %d\n", page.RootRecordPageNo)
	fmt.Printf("Freelist Pgno: %d\n", page.FreelistPageNo)
}

func printRootRecordPage(page *RootRecordPage) {
	fmt.Printf("Pgno: %d\n", page.Pgno)
	fmt.Printf("Type: root record\n")
	fmt.Printf("Next: %d\n", page.Next)
	fmt.Printf("Records: n=%d\n", len(page.Records))
	for i, rec := range page.Records {
		fmt.Printf("[%d]: name=%q pgno=%d\n", i, rec.Name, rec.Pgno)
	}
}

func printLeafPage(page *LeafPage) {
	fmt.Printf("Pgno: %d\n", page.Pgno)
	fmt.Printf("Type: leaf\n")
	fmt.Printf("Cells: n=%d\n", len(page.Cells))
	for i, cell := range page.Cells {
		if cell.Type == ContainerTypeBitmapPtr {
			fmt.Printf("[%d]: ckey=%d type=%s pgno=%d\n", i, cell.Key, cell.Type, cell.Pgno)
		} else {
			fmt.Printf("[%d]: ckey=%d type=%s values=%v\n", i, cell.Key, cell.Type, cell.Values)
		}
	}
}

func printBranchPage(page *BranchPage) {
	fmt.Printf("Pgno: %d\n", page.Pgno)
	fmt.Printf("Type: branch\n")
	fmt.Printf("Cells: n=%d\n", len(page.Cells))
	for i, cell := range page.Cells {
		fmt.Printf("[%d]: ckey=%d flags=%d pgno=%d\n", i, cell.Key, cell.Flags, cell.Pgno)
	}
}

func printBitmapPage(page *BitmapPage) {
	fmt.Printf("Pgno: %d\n", page.Pgno)
	fmt.Printf("Type: bitmap\n")
	fmt.Printf("Values: %v\n", page.Values)
}

func printFreePage(page *FreePage) {
	fmt.Printf("Pgno: %d\n", page.Pgno)
	fmt.Printf("Type: free\n")
}

func prefixToString(s string) (ret string) {
	defer func() {
		if err := recover(); err != nil {
			ret = s
		}
	}()
	return txkey.PrefixToString([]byte(s))
}

///////////////// happy linter

var _ = printMetaPage
var _ = printRootRecordPage
var _ = printLeafPage
var _ = printBranchPage
var _ = printBitmapPage
var _ = printFreePage
var _ = prefixToString
