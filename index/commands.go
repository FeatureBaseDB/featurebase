package index

import (
	"encoding/json"
    "log"
    "fmt"
)

type Rank struct {
	Key, Count uint64
}

type RankList []Rank

func (p RankList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p RankList) Len() int           { return len(p) }
func (p RankList) Less(i, j int) bool { return p[i].Count > p[j].Count }


type Command interface {
	Execute(*Fragment)string
    QueryType() string
    Response() string
    ResponseChannel() chan string
}


func BuildCommandFactory(req *RequestJSON,decoder *json.Decoder)Command{
    log.Println(req)
    var result Command

    switch req.Request{
    default:
        result=&CmdUnknown{make(chan string),req.Request}
    case "UnionCount":
        result= NewUnion(decoder)
    case "IntersectCount":
        result= NewIntersect(decoder)
    }
    return result
 }


type CmdUnknown struct{
    result chan string
    response string
}
func (cmd *CmdUnknown) Execute(f *Fragment)string {
    return fmt.Sprintf(`{ "Unknown Command":"%s" }`,cmd.response)
}
func (cmd *CmdUnknown) QueryType()string {
    return "UnknownCommand"
}
func (cmd *CmdUnknown) Response()string {
    return  <-cmd.result
}
func (cmd *CmdUnknown) ResponseChannel()chan string {
    return cmd.result
}

type CmdUnion struct{
    result chan string
    bitmap_ids []uint64
}
type Args struct {
    Bitmaps[] uint64
}


func NewUnion(decoder *json.Decoder) *CmdUnion{
    var f Args
    decoder.Decode(&f)

    result:= &CmdUnion{make(chan string),f.Bitmaps}
    return result
}
func (cmd *CmdUnion) Execute(f *Fragment)string {
    bm:=f.impl.Union(cmd.bitmap_ids)
    result := BitCount(bm)
    return fmt.Sprintf(`{ "value":%d }`,result)
}
func (cmd *CmdUnion) QueryType()string {
    return "UnionCount"
}
func (cmd *CmdUnion) Response()string {
    return  <-cmd.result
}
func (cmd *CmdUnion) ResponseChannel()chan string {
    return cmd.result
}


type CmdIntersect struct{
    result chan string
    bitmaps []uint64
}

func NewIntersect(decoder *json.Decoder) *CmdIntersect{
    var f Args
    decoder.Decode(&f)
    
    result:= &CmdIntersect{ make(chan string), f.Bitmaps }
    return result
}
func (cmd *CmdIntersect) Execute(f *Fragment)string {
    bm:=f.impl.Intersect(cmd.bitmaps)
    result := BitCount(bm)
    return fmt.Sprintf(`{ "value":%d }`,result)
}
func (cmd *CmdIntersect) QueryType()string {
    return "IntersectCount"
}
func (cmd *CmdIntersect) Response()string {
    return  <-cmd.result
}
func (cmd *CmdIntersect) ResponseChannel()chan string {
    return cmd.result
}
/*
type CmdSetBit struct{
    result chan string
    id uint64
    bit_pos uint64
}

func NewSetBit(decoder *json.Decoder) *CmdSetBit{
    var f interface{}
    decoder.Decode(&f)
    m := f.(map[string]interface{})


    result:= &CmdSetBit{make(chan string),f["bitmap_id"].(uint64),f["bit_pos"].(uint64)}
    return result
}
func (cmd *CmdSetBit) Execute(f *Fragment)string {
    bitmap := Get(f.impl,cmd.id)
    SetBit(bitmap,cmd.bit_pos)
    result := BitCount(bm)
    return fmt.Sprintf(`{ "value":%d }`,result)
}
func (cmd *CmdSetBit) QueryType()string {
    return "SetBit"
}
func (cmd *CmdSetBit) Response()string {
    return  <-cmd.result
}
func (cmd *CmdSetBit) ResponseChannel()chan string {
    return cmd.result
}
*/
