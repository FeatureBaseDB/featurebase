package extendiblehash

import (
	"strconv"
	"testing"

	"github.com/featurebasedb/featurebase/v3/bufferpool"
	"github.com/stretchr/testify/assert"
)

func makeDirectory() (*ExtendibleHashTable, error) {
	diskManager := bufferpool.NewInMemDiskSpillingDiskManager(128)
	bufferPool := bufferpool.NewBufferPool(128, diskManager)

	keySize := 12
	valueSize := 20

	return NewExtendibleHashTable(keySize, valueSize, bufferPool)
}

func TestHashTable_ExtendibleHash(t *testing.T) {
	d, err := makeDirectory()
	if err != nil {
		t.Fatal(err)
	}
	d.globalDepth = 4

	key := "321"  //   0011
	key2 := "123" //  1011

	result := d.hashFunction(Key(key))
	result2 := d.hashFunction(Key(key2))

	assert.Equal(t, 7, result)
	assert.Equal(t, 6, result2)
}

func TestHashTable_GetPage(t *testing.T) {
	d, err := makeDirectory()
	if err != nil {
		t.Fatal(err)
	}
	d.globalDepth = 4
	d.directory = make([]bufferpool.PageID, 16)

	key := "478"
	d.directory[14] = 2

	pageID, err := d.getPageID([]byte(key))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, int(pageID))
}

func TestHashTable_GetPage_ShouldReturnError_WhenOffsetIsNotLimitedToDataSize(t *testing.T) {
	d, err := makeDirectory()
	if err != nil {
		t.Fatal(err)
	}
	d.globalDepth = 4
	key := "478"

	_, err = d.getPageID([]byte(key))
	assert.Error(t, err)
}

func TestHashTable_GetPage_ShouldReturnError_WhenPageIDIsOutOfTheTable(t *testing.T) {
	d, err := makeDirectory()
	if err != nil {
		t.Fatal(err)
	}
	d.directory = make([]bufferpool.PageID, 0)
	key := "123"

	_, err = d.getPageID([]byte(key))
	assert.Error(t, err)
}

func TestHashTable_Get(t *testing.T) {
	d, err := makeDirectory()
	if err != nil {
		t.Fatal(err)
	}
	d.globalDepth = 4
	d.directory = make([]bufferpool.PageID, 16)

	d.directory[14] = 2

	// force there to be two pages
	page, err := d.bufferPool.NewPage() //1
	if err != nil {
		t.Fatal(err)
	}
	page.WritePageType(bufferpool.PAGE_TYPE_HASH_TABLE)
	d.bufferPool.FlushPage(page.ID())

	page, err = d.bufferPool.NewPage() //2
	if err != nil {
		t.Fatal(err)
	}
	page.WritePageType(bufferpool.PAGE_TYPE_HASH_TABLE)
	d.bufferPool.FlushPage(page.ID())

	// now do the test
	page, err = d.bufferPool.FetchPage(2)
	if err != nil {
		t.Fatal(err)
	}
	defer d.bufferPool.UnpinPage(page.ID())

	key := "478"
	value := "Hi"

	page.WriteKeyValueInSlot(0, []byte(key), []byte(value))
	page.WriteSlotCount(int16(1))

	result, _, err := d.Get([]byte(key))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "Hi", string(result))
}

func TestHashTable_Get_ShouldHandleError(t *testing.T) {
	d, err := makeDirectory()
	if err != nil {
		t.Fatal(err)
	}
	key := "123"

	result, found, err := d.Get([]byte(key))

	assert.Equal(t, err, nil)
	assert.Equal(t, []byte{}, result)
	assert.Equal(t, false, found)
}

func TestHashTable_Put(t *testing.T) {
	d, err := makeDirectory()
	if err != nil {
		t.Fatal(err)
	}

	page, err := d.bufferPool.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.bufferPool.UnpinPage(page.ID())
	err = addToPage(page, 5)
	if err != nil {
		t.Fatal(err)
	}

	d.Put([]byte("123"), []byte("Yolo !"))

	value, found, err := d.Get([]byte("123"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "Yolo !", string(value))
	assert.Equal(t, true, found)
}

func TestHashTable_Put_ShouldIncreaseSize_WhenTableIsFull(t *testing.T) {
	d, err := makeDirectory()
	if err != nil {
		t.Fatal(err)
	}

	page, err := d.bufferPool.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.bufferPool.UnpinPage(page.ID())
	err = addToPage(page, 227) // keys per page with key 12, value 20
	if err != nil {
		t.Fatal(err)
	}

	d.Put([]byte("123"), []byte("Yolo !"))

	value, _, err := d.Get([]byte("123"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "Yolo !", string(value))
	assert.Equal(t, 2, len(d.directory))
	assert.Equal(t, uint(1), d.globalDepth)
}

func TestHashTable_PutShouldIncrementLD_WhenPageIsFull(t *testing.T) {
	d, err := makeDirectory()
	if err != nil {
		t.Fatal(err)
	}

	page, err := d.bufferPool.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.bufferPool.UnpinPage(page.ID())
	err = addToPage(page, 227) // keys per page with key 12, value 20
	if err != nil {
		t.Fatal(err)
	}

	d.Put([]byte("12345678"), []byte("Yolo !"))

	assert.Equal(t, int64(8192*2), d.bufferPool.OnDiskSize())
	assert.Equal(t, 1, int(d.globalDepth))

	p0, err := d.bufferPool.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.bufferPool.UnpinPage(p0.ID())
	assert.Equal(t, int16(1), p0.ReadLocalDepth())

	p1, err := d.bufferPool.FetchPage(1)
	if err != nil {
		t.Fatal(err)
	}
	defer d.bufferPool.UnpinPage(p1.ID())
	assert.Equal(t, int16(1), p1.ReadLocalDepth())
}

func TestHashTable_Put_INT(t *testing.T) {
	d, err := makeDirectory()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 4000; i++ {
		err = d.Put([]byte("key"+strconv.Itoa(i)), []byte("Yolo !"))
		if err != nil {
			t.Fatal(err)
		}
	}

	assert.Equal(t, []bufferpool.PageID{0, 1, 2, 3, 4, 7, 6, 5, 13, 14, 12, 9, 8, 15, 10, 11, 28, 24, 21, 18, 4, 19, 29, 20, 27, 22, 25, 23, 17, 15, 16, 26}, d.directory)
}

func TestHashTable_Put_SameKey_ALotOfTime(t *testing.T) {
	d, err := makeDirectory()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10000; i++ {
		d.Put([]byte("key"), []byte("Yolo ! "+strconv.Itoa(i)))
	}

	value, _, err := d.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "Yolo ! 9999", string(value))
	assert.Equal(t, 1, len(d.directory))
	assert.Equal(t, int64(8192), d.bufferPool.OnDiskSize())
}

func TestHashTable_Put_Many_Keys(t *testing.T) {
	d, err := makeDirectory()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000000; i++ {
		err = d.Put([]byte("key"+strconv.Itoa(i)), []byte("Yolo ! "+strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	value, _, err := d.Get([]byte("key99756"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "Yolo ! 99756", string(value))
	assert.Equal(t, 8192, len(d.directory))
	assert.Equal(t, uint(13), d.globalDepth)
	d.Close()
}

func BenchmarkHashTable_Put_Many_Keys(b *testing.B) {
	for i := 0; i < b.N; i++ {

		d, err := makeDirectory()
		if err != nil {
			b.Fatal(err)
		}

		for i := 0; i < 1000000; i++ {
			err = d.Put([]byte("key"+strconv.Itoa(i)), []byte("Yolo ! "+strconv.Itoa(i)))
			if err != nil {
				b.Fatal(err)
			}
		}

		value, _, err := d.Get([]byte("key99756"))
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, "Yolo ! 99756", string(value))
		assert.Equal(b, 8192, len(d.directory))
		assert.Equal(b, uint(13), d.globalDepth)
		d.Close()

	}
}

func addToPage(page *bufferpool.Page, numberOfRecords int) error {
	for i := 0; i < numberOfRecords; i++ {
		//fmt.Printf("writing record %d\n", i+1)
		itoa := strconv.Itoa(i)
		err := page.WriteKeyValueInSlot(int16(i), []byte("key"+itoa), []byte("value foo bar"))
		if err != nil {
			return err
		}
		page.WriteSlotCount(int16(page.ReadSlotCount() + 1))
	}
	return nil
}
