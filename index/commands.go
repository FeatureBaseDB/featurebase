package index

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"time"
)

type Result struct {
	answer    Calculation
	exec_time time.Duration
}

type Responder struct {
	result     chan Result
	query_type string
}

func NewResponder(query_type string) *Responder {
	return &Responder{make(chan Result), query_type}
}
func (self *Responder) QueryType() string {
	return self.query_type
}
func (self *Responder) Response() Result {
	return <-self.result
}
func (self *Responder) ResponseChannel() chan Result {
	return self.result
}

type Calculation interface{}

type Command interface {
	Execute(*Fragment) Calculation
	QueryType() string
	Response() Result
	ResponseChannel() chan Result
}

type CmdGet struct {
	*Responder
	bitmap_id uint64
}

func NewGet(bitmap_id uint64) *CmdGet {
	return &CmdGet{NewResponder("Get"), bitmap_id}
}

func (self *CmdGet) Execute(f *Fragment) Calculation {
	return f.NewHandle(self.bitmap_id)
}

type CmdCount struct {
	*Responder
	bitmap BitmapHandle
}

func NewCount(bitmap_handle BitmapHandle) *CmdCount {
	return &CmdCount{NewResponder("Count"), bitmap_handle}
}

func (self *CmdCount) Execute(f *Fragment) Calculation {
	bm, _ := f.getBitmap(self.bitmap)
	return BitCount(bm)
}

type CmdUnion struct {
	*Responder
	bitmap_ids []BitmapHandle
}

func NewUnion(bitmaps []BitmapHandle) *CmdUnion {
	result := &CmdUnion{NewResponder("Union"), bitmaps}
	return result
}
func (self *CmdUnion) Execute(f *Fragment) Calculation {
	return f.union(self.bitmap_ids)
}

type CmdDifference struct {
	*Responder
	bitmap_ids []BitmapHandle
}

func NewDifference(bitmaps []BitmapHandle) *CmdDifference {
	result := &CmdDifference{NewResponder("Difference"), bitmaps}
	return result
}
func (self *CmdDifference) Execute(f *Fragment) Calculation {
	return f.difference(self.bitmap_ids)
}

type CmdIntersect struct {
	*Responder
	bitmaps []BitmapHandle
}

func NewIntersect(bh []BitmapHandle) *CmdIntersect {
	return &CmdIntersect{NewResponder("Intersect"), bh}
}
func (self *CmdIntersect) Execute(f *Fragment) Calculation {
	return f.intersect(self.bitmaps)
}

type BitArgs struct {
	Bitmap_id uint64
	Bit_pos   uint64
}
type CmdSetBit struct {
	*Responder
	bitmap_id uint64
	bit_pos   uint64
	filter    uint64
}

func NewSetBit(bitmap_id uint64, bit_pos uint64, filter uint64) *CmdSetBit {
	result := &CmdSetBit{NewResponder("SetBit"), bitmap_id, bit_pos, filter}
	return result
}
func (self *CmdSetBit) Execute(f *Fragment) Calculation {
	return f.impl.SetBit(self.bitmap_id, self.bit_pos, self.filter)
}

type CmdGetBytes struct {
	*Responder
	bitmap BitmapHandle
}

func NewGetBytes(bh BitmapHandle) *CmdGetBytes {
	return &CmdGetBytes{NewResponder("GetBytes"), bh}
}

func (self *CmdGetBytes) Execute(f *Fragment) Calculation {
	bm, _ := f.getBitmap(self.bitmap)
	//*Compress it
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(bm.ToBytes())
	w.Flush()
	w.Close()

	return b.Bytes()
}

type CmdFromBytes struct {
	*Responder
	compressed_bytes []byte
}

func NewFromBytes(bytes []byte) *CmdFromBytes {
	return &CmdFromBytes{NewResponder("FromBytes"), bytes}
}

func (self *CmdFromBytes) Execute(f *Fragment) Calculation {
	reader, _ := gzip.NewReader(bytes.NewReader(self.compressed_bytes))
	b, _ := ioutil.ReadAll(reader)

	result := NewBitmap()
	result.FromBytes(b)
	return f.AllocHandle(result)
}

type CmdEmpty struct {
	*Responder
}

func NewEmpty() *CmdEmpty {
	return &CmdEmpty{NewResponder("Empty")}
}

func (self *CmdEmpty) Execute(f *Fragment) Calculation {
	result := NewBitmap()
	return f.AllocHandle(result)
}

type CmdGetList struct {
	*Responder
	bitmap_ids []uint64
}

func NewGetList(bitmap_ids []uint64) *CmdGetList {
	return &CmdGetList{NewResponder("GetList"), bitmap_ids}
}

func (self *CmdGetList) Execute(f *Fragment) Calculation {
	ret := make([]BitmapHandle, len(self.bitmap_ids))
	for i, v := range self.bitmap_ids {
		ret[i] = f.NewHandle(v)
	}

	return ret
}

type CmdTopN struct {
	*Responder
	bitmap     BitmapHandle
	n          int
	categories []uint64
}

func NewTopN(b BitmapHandle, n int, categories []uint64) *CmdTopN {
	return &CmdTopN{NewResponder("TopN"), b, n, categories}
}
func (self *CmdTopN) Execute(f *Fragment) Calculation {
	return f.TopN(self.bitmap, self.n, self.categories)
}

type CmdClear struct {
	*Responder
}

func NewClear() *CmdClear {
	return &CmdClear{NewResponder("Clear")}
}
func (self *CmdClear) Execute(f *Fragment) Calculation {
	return f.impl.Clear()
}

type CmdLoader struct {
	*Responder
	bitmap_id         uint64
	compressed_bitmap string
	filter            uint64
}

func NewLoader(bitmap_id uint64, compressed_bitmap string, filter uint64) *CmdLoader {
	return &CmdLoader{NewResponder("Loader"), bitmap_id, compressed_bitmap, filter}
}
func (self *CmdLoader) Execute(f *Fragment) Calculation {
	nbm := NewBitmap()
	nbm.FromCompressString(self.compressed_bitmap)
	f.impl.Store(self.bitmap_id, nbm, self.filter)
	return "ok"
}

type CmdStats struct {
	*Responder
}

func NewStats() *CmdStats {
	return &CmdStats{NewResponder("Stats")}
}
func (self *CmdStats) Execute(f *Fragment) Calculation {
	return f.impl.Stats()
}

type CmdLoadRequest struct {
	*Responder
	bitmap_id uint64
}

func NewLoadRequest(bitmap_id uint64) *CmdLoadRequest {
	result := &CmdLoadRequest{NewResponder("LoadRequest"), bitmap_id}
	return result
}
func (self *CmdLoadRequest) Execute(f *Fragment) Calculation {
	f.impl.Get(self.bitmap_id)
	return 0
}

type CmdRange struct {
	*Responder
	bitmap_id  uint64
	start_time time.Time
	end_time   time.Time
}

func NewRange(bitmap_id uint64, start, end time.Time) *CmdRange {
	return &CmdRange{NewResponder("Range"), bitmap_id, start, end}
}

func (self *CmdRange) Execute(f *Fragment) Calculation {
	return f.build_time_range_bitmap(self.bitmap_id, self.start_time, self.end_time)
}
