package bufferpool

import (
	"errors"
	"fmt"
	"os"

	uuid "github.com/satori/go.uuid"
)

// InMemDiskSpillingDiskManager is a memory implementation for a DiskManager interface
// that can spill to disk when a threshold is reached
type InMemDiskSpillingDiskManager struct {
	// tracks the number of pages
	numPages int

	onDiskPages int

	// tracks the number of pages we can consume before spilling
	thresholdPages int
	hasSpilled     *struct{}
	fd             *os.File

	// the data buffer
	data []byte
}

// NewInMemDiskSpillingDiskManager returns a in-memory version of disk manager
func NewInMemDiskSpillingDiskManager(thresholdPages int) *InMemDiskSpillingDiskManager {
	dm := &InMemDiskSpillingDiskManager{
		numPages:       0,
		thresholdPages: thresholdPages,
		data:           make([]byte, 0),
	}
	return dm
}

// ReadPage reads a page from pages
func (d *InMemDiskSpillingDiskManager) ReadPage(pageID PageID) (*Page, error) {
	// check we're not asking for page out of range
	if pageID < 0 || int(pageID) >= d.numPages {
		return nil, errors.New("page not found")
	}
	// check that the offset is within range
	offset := int(pageID) * PAGE_SIZE

	var page = pageSyncPool.Get().(*Page)
	// we have to do this stupid check because if -cpuprofile is set for go test, this
	// the previous line return a weird nil-ish thing...
	if page == (*Page)(nil) {
		page = pageSyncPool.New().(*Page)
	}
	page.id = pageID

	// do the read
	if d.hasSpilled == nil {
		if offset+PAGE_SIZE > len(d.data) {
			return nil, errors.New("offset out of range")
		}
		b := copy(page.data[:], d.data[offset:offset+PAGE_SIZE])
		fmt.Printf("bytes read: %d", b)
	} else {
		var err error
		if offset+PAGE_SIZE > d.numPages*PAGE_SIZE {
			return nil, errors.New("offset out of range")
		}
		_, err = d.fd.ReadAt(page.data[:], int64(offset))
		if err != nil {
			return nil, err
		}
	}
	return page, nil
}

// WritePage writes a page in memory to pages
func (d *InMemDiskSpillingDiskManager) WritePage(page *Page) error {
	// make sure the offset is sensible
	offset := int(page.ID()) * PAGE_SIZE
	// do the write
	if d.hasSpilled == nil {
		if offset+PAGE_SIZE > len(d.data) {
			return errors.New("offset out of range")
		}
		copy(d.data[offset:], page.data[:])
	} else {
		var err error
		if offset+PAGE_SIZE > d.numPages*PAGE_SIZE {
			return errors.New("offset out of range")
		}
		_, err = d.fd.WriteAt(page.data[:], int64(offset))
		if err != nil {
			return err
		}
		// err = d.fd.Sync()
		// if err != nil {
		// 	return err
		// }
	}
	return nil
}

// AllocatePage allocates a page and returns the page number
func (d *InMemDiskSpillingDiskManager) AllocatePage() (PageID, error) {
	d.numPages = d.numPages + 1
	pageID := PageID(d.numPages - 1)

	if d.hasSpilled == nil {
		// we have not spilled (yet), so make storage bigger
		newData := make([]byte, PAGE_SIZE)
		d.data = append(d.data, newData...)

		// check to see if we need to spill
		if d.numPages > d.thresholdPages {
			fileUUID, err := uuid.NewV4()
			if err != nil {
				return PageID(INVALID_PAGE), err
			}
			// TODO(pok) we should try to tell the OS not to cache this file
			d.fd, err = os.CreateTemp("", fmt.Sprintf("fb-ehash-%s", fileUUID.String()))
			if err != nil {
				return PageID(INVALID_PAGE), err
			}
			_, err = d.fd.WriteAt(d.data, 0)
			if err != nil {
				return PageID(INVALID_PAGE), err
			}
			d.data = []byte{}
			d.hasSpilled = &struct{}{}
		}
	} else {
		if d.numPages >= d.onDiskPages {
			// grow the file by a chunk - 512 pages
			d.onDiskPages += 512
			var err error
			size := int64(d.onDiskPages * PAGE_SIZE)
			_, err = d.fd.WriteAt([]byte{0}, size-1)
			if err != nil {
				return PageID(INVALID_PAGE), err
			}
		}
	}

	return pageID, nil
}

// DeallocatePage removes page from disk
func (d *InMemDiskSpillingDiskManager) DeallocatePage(pageID PageID) error {
	// nothing to do right now
	return nil
}

func (d *InMemDiskSpillingDiskManager) FileSize() int64 {
	return int64(len(d.data))
}

func (d *InMemDiskSpillingDiskManager) Close() {
	// close and delete the file if we spilled
	if d.fd != nil {
		_ = d.fd.Close()
		os.Remove(d.fd.Name())
	}
}
