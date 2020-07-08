Roaring B-tree Format
=====================

The RBF format represents a Roaring bitmap whose containers are stored in the
leafs of a b-tree. This allows the bitmap to be efficiently queried & updated.


## File Format

The RBF file is divided into equal 8KB pages. Each page after the meta page
is numbered incrementally from 1 to 1^31.

Pages can be one of the following types:

- Meta page: contains header information.
- Branch page: contains pointers to lower branch & leaf pages.
- Leaf page: contains array and RLE container data.
- Bitmap page: contains bitmap container data.

All integer values are little endian encoded.


## Page header

Every page type except the bitmap page contains the following header:


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
	[*] array or RLE data


### Bitmap page

The data for the bitmap page takes up the entire 8KB.


## Proof of Concept Notes

The following are notes made that are temporary for the RBF format. This will
change as development progresses:

- Transaction support is deferred
- WAL support is deferred


