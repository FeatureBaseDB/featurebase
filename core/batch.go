package core

import (
	"encoding/gob"
	"pilosa/db"
	. "pilosa/util"

	"github.com/gocql/gocql/uuid"
)

type BatchRequest struct {
	Id                *uuid.UUID
	Source            *uuid.UUID
	Fragment_id       SUUID
	Bitmap_id         uint64
	Compressed_bitmap string
	Filter            uint64
}

type BatchResponse struct {
	Id *uuid.UUID
}

func (self BatchResponse) ResultId() *uuid.UUID {
	return self.Id
}
func (self BatchResponse) ResultData() interface{} {
	return self.Id
}

func init() {
	gob.Register(BatchRequest{})
	gob.Register(BatchResponse{})
}

func (self *Service) Batch(database_name, frame, compressed_bitmap string, bitmap_id uint64, slice int, filter uint64) error {
	//determine the fragment_id from database/frame/slice
	database := self.Cluster.GetOrCreateDatabase(database_name)
	oslice := database.GetOrCreateSlice(slice)
	//need to find processid and fragment id for that slice

	fragment, err := database.GetFragmentForBitmap(oslice, &db.Bitmap{bitmap_id, frame, filter})
	if err == nil {
		id := uuid.RandomUUID()
		batch := db.Message{Data: BatchRequest{Id: &id, Source: self.Id, Fragment_id: fragment.GetId(), Bitmap_id: bitmap_id, Compressed_bitmap: compressed_bitmap}}
		dest_id := fragment.GetProcess().Id()
		self.Transport.Send(&batch, &dest_id)

		_, err = self.Hold.Get(&id, 60)
	}
	return err

}
