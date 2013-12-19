package index

import "time"

type Rank struct {
	Key, Count uint64
}

type RankList []Rank

func (p RankList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p RankList) Len() int           { return len(p) }
func (p RankList) Less(i, j int) bool { return p[i].Count > p[j].Count }

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
func (cmd *Responder) QueryType() string {
	return cmd.query_type
}
func (cmd *Responder) Response() Result {
	return <-cmd.result
}
func (cmd *Responder) ResponseChannel() chan Result {
	return cmd.result
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

func (cmd *CmdGet) Execute(f *Fragment) Calculation {
	return f.NewHandle(cmd.bitmap_id)
}

type CmdCount struct {
	*Responder
	bitmap BitmapHandle
}

func NewCount(bitmap_handle BitmapHandle) *CmdCount {
	return &CmdCount{NewResponder("Count"), bitmap_handle}
}

func (cmd *CmdCount) Execute(f *Fragment) Calculation {
	bm, _ := f.getBitmap(cmd.bitmap)
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
func (cmd *CmdUnion) Execute(f *Fragment) Calculation {
	return f.union(cmd.bitmap_ids)
}

type CmdIntersect struct {
	*Responder
	bitmaps []BitmapHandle
}

func NewIntersect(bh []BitmapHandle) *CmdIntersect {
	return &CmdIntersect{NewResponder("Intersect"), bh}
}
func (cmd *CmdIntersect) Execute(f *Fragment) Calculation {
	return f.intersect(cmd.bitmaps)
}

type BitArgs struct {
	Bitmap_id uint64
	Bit_pos   uint64
}
type CmdSetBit struct {
	*Responder
	bitmap_id uint64
	bit_pos   uint64
}

func NewSetBit(bitmap_id uint64, bit_pos uint64) *CmdSetBit {
	result := &CmdSetBit{NewResponder("SetBit"), bitmap_id, bit_pos}
	return result
}
func (cmd *CmdSetBit) Execute(f *Fragment) Calculation {
	return f.impl.SetBit(cmd.bitmap_id, cmd.bit_pos)
}

type CmdGetBytes struct {
	*Responder
	bitmap BitmapHandle
}

func NewGetBytes(bh BitmapHandle) *CmdGetBytes {
	return &CmdGetBytes{NewResponder("GetBytes"), bh}
}

func (cmd *CmdGetBytes) Execute(f *Fragment) Calculation {
	bm, _ := f.getBitmap(cmd.bitmap)
	return bm.ToBytes()
}

type CmdFromBytes struct {
	*Responder
	bytes []byte
}

func NewFromBytes(bytes []byte) *CmdFromBytes {
	return &CmdFromBytes{NewResponder("FromBytes"), bytes}
}

func (cmd *CmdFromBytes) Execute(f *Fragment) Calculation {
	result := NewBitmap()
	result.FromBytes(cmd.bytes)
	return f.AllocHandle(result)
}
