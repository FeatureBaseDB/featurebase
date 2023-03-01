package bufferpool

// DiskManager is responsible for interacting with disk
type DiskManager interface {
	// reads a page from the disk
	ReadPage(PageID) (*Page, error)

	// writes a page to the disk
	WritePage(*Page) error

	// allocates a page
	AllocatePage(objectID int32, shard int32) (PageID, error)

	// deallocates a page
	DeallocatePage(PageID) error

	// returns on disk file size
	FileSize(objectID int32, shard int32) int64

	// closes and does any clean up
	Close()
}
