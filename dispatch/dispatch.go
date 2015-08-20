package dispatch

import (
	log "github.com/cihub/seelog"
	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa/core"
	"github.com/umbel/pilosa/db"
	"github.com/umbel/pilosa/executor"
	"github.com/umbel/pilosa/index"
	"github.com/umbel/pilosa/query"
	"github.com/umbel/pilosa/util"
)

type Dispatch struct {
	Executor interface {
		NewJob(job *db.Message)
	}

	Hold interface {
		Set(id *util.GUID, value interface{}, timeout int)
	}

	Index interface {
		ClearBit(fragID util.SUUID, bitmapID uint64, pos uint64) (bool, error)
		LoadBitmap(fragID util.SUUID, bitmapID uint64, compressedBitmap string, filter uint64)
		SetBit(fragID util.SUUID, bitmapID uint64, pos uint64, category uint64) (bool, error)
		TopFillBatch(args []index.FillArgs) ([]index.Pair, error)
	}

	Transport interface {
		Receive() *db.Message
		Send(message *db.Message, host *util.GUID)
	}
}

func NewDispatch() *Dispatch {
	return &Dispatch{}
}

func (self *Dispatch) Init() error {
	log.Warn("Starting Dispatcher")
	return nil
}

func (self *Dispatch) Close() {
	log.Warn("Shutting down Dispatcher")
}

// The Local Route

func (d *Dispatch) Run() {
	log.Warn("Dispatch Run...")
	for {
		message := d.Transport.Receive()

		switch data := message.Data.(type) {
		case core.BatchRequest:
			log.Trace("Dispatch.Run BatchRequest")
			response := db.Message{Data: core.BatchResponse{Id: data.Id}}
			d.Index.LoadBitmap(data.Fragment_id, data.Bitmap_id, data.Compressed_bitmap, data.Filter)
			d.Transport.Send(&response, data.Source)

		case core.BitsRequest:
			log.Trace("Dispatch.Run BitsRequest")
			var results []core.SBResult
			result := false

			for _, v := range data.Bits {
				if v.SetUnset {
					result, _ = d.Index.SetBit(v.Fragment_id, v.Bitmap_id, v.Profile_id, uint64(v.Filter))
				} else {
					result, _ = d.Index.ClearBit(v.Fragment_id, v.Bitmap_id, v.Profile_id)
				}
				bundle := core.SBResult{Bitmap_id: v.Bitmap_id, Frame: v.Frame, Filter: v.Filter, Profile_id: v.Profile_id, Result: result}
				results = append(results, bundle)
			}
			response := db.Message{Data: core.BitsResponse{Id: &data.QueryId, Items: results}}
			d.Transport.Send(&response, &data.ReturnProcessId)

		case core.PingRequest:
			log.Trace("Dispatch.Run Ping")
			pong := db.Message{Data: core.PongRequest{Id: data.Id}}
			d.Transport.Send(&pong, data.Source)

		case db.HoldResult:
			log.Trace("Dispatch.Run HoldResult")
			d.Hold.Set(data.ResultId(), data.ResultData(), 30)

		case query.PortableQueryStep:
			log.Trace("Dispatch.Run PortableQueryStep")
			go d.Executor.NewJob(message)

		case executor.TopFill:
			log.Trace("Dispatch.Run TopFill")
			go d.topFillHandler(message)

		case core.BitsResponse:
			d.Hold.Set(data.ResultId(), data.ResultData(), 30)

		default:
			spew.Dump(data)
			log.Warn("Unprocessed message", data)
		}
	}
}

func (d *Dispatch) topFillHandler(m *db.Message) {
	topfill := m.Data.(executor.TopFill)
	topn, err := d.Index.TopFillBatch(topfill.Args)
	if err != nil {
		log.Warn("TopFillHandler:", err)
	}

	d.Transport.Send(&db.Message{
		Data: query.FillResult{
			BaseQueryResult: &query.BaseQueryResult{
				Id:   &topfill.QueryId,
				Data: topn,
			},
		},
	}, &topfill.ReturnProcessId)
}
