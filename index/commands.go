package index

import (
	"encoding/json"
	"fmt"
)

type Rank struct {
	Key, Count uint64
}

type RankList []Rank

func (p RankList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p RankList) Len() int           { return len(p) }
func (p RankList) Less(i, j int) bool { return p[i].Count > p[j].Count }

type Responder struct {
	result     chan string
	query_type string
}

func NewResponder(query_type string) *Responder {
	return &Responder{make(chan string), query_type}
}
func (cmd *Responder) QueryType() string {
	return cmd.query_type
}
func (cmd *Responder) Response() string {
	return <-cmd.result
}
func (cmd *Responder) ResponseChannel() chan string {
	return cmd.result
}

type Command interface {
	Execute(*Fragment) string
	GetResponder() *Responder
}

func BuildCommandFactory(req *RequestJSON, decoder *json.Decoder) Command {
	var result Command

	switch req.Request {
	default:
		result = &CmdUnknown{NewResponder("UnknownCommand"), req.Request}
	case "UnionCount":
		result = NewUnion(decoder)
	case "IntersectCount":
		result = NewIntersect(decoder)
	case "SetBit":
		result = NewSetBit(decoder)
	}
	return result
}

type CmdUnknown struct {
	meta     *Responder
	response string
}

func (cmd *CmdUnknown) Execute(f *Fragment) string {
	return fmt.Sprintf(`{ "Unknown Command":"%s" }`, cmd.response)
}

func (cmd *CmdUnknown) GetResponder() *Responder {
	return cmd.meta
}

type CmdUnion struct {
	meta       *Responder
	bitmap_ids []uint64
}
type Args struct {
	Bitmaps []uint64
}

func NewUnion(decoder *json.Decoder) *CmdUnion {
	var f Args
	decoder.Decode(&f)
	result := &CmdUnion{NewResponder("UnionCount"), f.Bitmaps}
	return result
}
func (cmd *CmdUnion) Execute(f *Fragment) string {
	bm := f.impl.Union(cmd.bitmap_ids)
	result := BitCount(bm)
	return fmt.Sprintf(`{ "value":%d }`, result)
}
func (cmd *CmdUnion) GetResponder() *Responder {
	return cmd.meta
}

type CmdIntersect struct {
	meta    *Responder
	bitmaps []uint64
}

func NewIntersect(decoder *json.Decoder) *CmdIntersect {
	var f Args
	decoder.Decode(&f)

	result := &CmdIntersect{NewResponder("IntersectCount"), f.Bitmaps}
	return result
}
func (cmd *CmdIntersect) Execute(f *Fragment) string {
	bm := f.impl.Intersect(cmd.bitmaps)
	result := BitCount(bm)
	return fmt.Sprintf(`{ "value":%d }`, result)
}
func (cmd *CmdIntersect) GetResponder() *Responder {
	return cmd.meta
}

type BitArgs struct {
	Bitmap_id uint64
	Bit_pos   uint64
}

type CmdSetBit struct {
	meta *Responder

	id      uint64
	bit_pos uint64
}

func NewSetBit(decoder *json.Decoder) *CmdSetBit {
	var f BitArgs
	decoder.Decode(&f)
	result := &CmdSetBit{NewResponder("SetBit"), f.Bitmap_id, f.Bit_pos}
	return result
}
func (cmd *CmdSetBit) Execute(f *Fragment) string {
	bitmap := f.impl.Get(cmd.id)
	val := SetBit(bitmap, cmd.bit_pos)
	m := 0
	if val {
		m = 1
	}
	result := BitCount(bitmap)
	return fmt.Sprintf(`{ "value":%d , "changed":%d}`, result, m)
}
func (cmd *CmdSetBit) GetResponder() *Responder {
	return cmd.meta
}
