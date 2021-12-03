Roaring B-tree Format
=====================

The RBF format represents a Roaring bitmap whose containers are stored in the
leafs of a b-tree. This allows the bitmap to be efficiently queried & updated.


## File Format

The RBF file is divided into equal 8KB pages. Each page after the meta page
is numbered incrementally from 1 to 2^31.

Pages can be one of the following types:

- Meta page: contains header information.
- Branch page: contains pointers to lower branch & leaf pages.
- Leaf page: contains array and RLE container data.
- Bitmap page: contains bitmap container data.

All integer values are little endian encoded.


## Page header

Every page type except the bitmap page contains the following header:

	[4] page number
	[4] flags (indicates the type of the page)

### Meta page

The meta page contains the following header:

	[4]  magic (\xFFRBF)
	[4]  flags
	[4]  page count
	[8]  wal ID
	[4]  root records pgno
	[4]  freelist pgno


### Root Records page

A list of all b-tree names & their respective root page numbers are stored in
root record pages. Once a bitmap root is created, it is never moved so the 
root record pages only need to be rewritten when creating, renaming, or deleting
a b-tree. If records exceed the size of a page then they are overflowed to
additional pages.

	[4] page number
	[4] flags
	[4] overflow pgno
	[*] bitmap records

Each bitmap record is represented as:

	[4] pgno
	[2] name size
	[*] name

All bitmap records are loaded into memory when the file is opened.


### Branch page

The branch page contains the following header:

	[4] page number
	[4] flags
	[2] cell count
	[*] cell index (2 * cell count)
	[*] padding for 4-byte alignment

Each cell is formatted as:

	[8] highbits
	[4] flags
	[4] page number


### Leaf page

The leaf page contains the following header:

	[4] page number
	[4] flags
	[2] cell count
	[*] cell index (2 * cell count)


The leaf page contains a series of cells with the header of:

	[8] highbits
	[4] flag
	[4] child count
	[*] array or RLE data or Handle (a pageno) to Bitmap Data


### Bitmap Data page

The data for the bitmap data page takes up the entire 8KB.


## Proof of Concept Notes

The following are notes made that are temporary for the RBF format. This will
change as development progresses:

- Transaction support is deferred
- WAL support is deferred


## Pattern of branch splits when adding data in ascending sorted order.

In this example, fan-out is restricted to 2 to make the
drawings easy and the splits obvious. The data leaves are only allowed one
roaring.Container key (ckey) in this example.

Each frame adds the next datum: A,B,C,D,E,... in order.

The letter represent data leaves, while
numbers represent branch pages. The one exception is the
first frame where the root is a leaf with data.
Every frame after has normal branch at the root.

NB: there are only three places pages get written on addition:
a) writeRoot
b) putLeafCell
c) putBranchCells

---------------------------------------------
add A:
        root
         3
         A
---------------------------------------------
add B:
      root
       3
     4   5
     A   B
---------------------------------------------
add C: this sequence of updates occurs

1) putLeafCell writes leaf B to page 5
2) putLeafCell writes leaf C to page 6
3) putBranchCells writes branch page 7 with children 4,5
4) putBranchCells writes branch page 8 with child 6
5) writeRoot writes branch cells to pgno 3, children: 7,8

      root
       3
    7     8
  4  5    6
  A  B    C
---------------------------------------------
add D:
      root                 
       3
    7      8
  4  5   6   9
  A  B   C   D
             
---------------------------------------------
add E:
             root
              3
     12              13
  7      8           11
4  5    6 9          10
A  B    C D           E
---------------------------------------------
add F:
             root
              3
     12              13
  7      8           11
4  5    6 9        10  14
A  B    C D         E   F
---------------------------------------------
add G:
             root
              3
     12                13
  7      8           11    16
4  5    6 9        10  14  15
A  B    C D         E   F   G
---------------------------------------------
add H:
             root
              3
     12                13
  7      8           11    16
4  5    6 9        10  14  15 17
A  B    C D         E   F   G  H
---------------------------------------------
add I:
                                 root
                                  3
              21                         22
     12                 13               20
  7      8           11    16            19
4  5    6 9        10  14  15 17         18
A  B    C D         E   F   G  H          I
---------------------------------------------

