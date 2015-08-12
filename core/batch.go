package core

import (
	"encoding/gob"

	log "github.com/cihub/seelog"
	"github.com/umbel/pilosa/db"
	"github.com/umbel/pilosa/hold"
	"github.com/umbel/pilosa/util"
)

type BatchRequest struct {
	Id                *util.GUID
	Source            *util.GUID
	Fragment_id       util.SUUID
	Bitmap_id         uint64
	Compressed_bitmap string
	Filter            uint64
}

type BatchResponse struct {
	Id *util.GUID
}

func (self BatchResponse) ResultId() *util.GUID {
	return self.Id
}
func (self BatchResponse) ResultData() interface{} {
	return self.Id
}

func init() {
	gob.Register(BatchRequest{})
	gob.Register(BatchResponse{})
}

type Batcher struct {
	ID      util.GUID
	Cluster *db.Cluster
	Hold    *hold.Holder

	Transport interface {
		Send(message *db.Message, host *util.GUID)
	}
}

func NewBatcher(id util.GUID) *Batcher {
	return &Batcher{ID: id}
}

func (b *Batcher) Batch(database_name, frame, compressed_bitmap string, bitmap_id uint64, slice int, filter uint64) error {
	log.Trace("Batch:", "db:", database_name, " frame:", frame, " slice:", slice, " cb:", compressed_bitmap, " bid:", bitmap_id, "f:", filter)

	//determine the fragment_id from database/frame/slice
	database := b.Cluster.GetOrCreateDatabase(database_name)
	oslice := database.GetOrCreateSlice(slice)
	//need to find processid and fragment id for that slice

	fragment, err := database.GetFragmentForBitmap(oslice, &db.Bitmap{bitmap_id, frame, filter})
	if err == nil {
		id := util.RandomUUID()
		batch := db.Message{Data: BatchRequest{Id: &id, Source: &b.ID, Fragment_id: fragment.GetId(), Bitmap_id: bitmap_id, Compressed_bitmap: compressed_bitmap}}
		dest_id := fragment.GetProcess().Id()
		b.Transport.Send(&batch, &dest_id)

		_, err = b.Hold.Get(&id, 60)
	}

	return err
}
