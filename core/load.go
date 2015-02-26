package core

// #cgo  CFLAGS:-mpopcnt

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	log "github.com/cihub/seelog"
	"pilosa/index"
)

func copy_raw(src [32]uint64) index.BlockArray {
	var o = make([]uint64, 32, 32)
	for k, v := range src {
		o[k] = v
	}
	return index.BlockArray{o}
}
func sendBitmap(service *Service, bitmap index.IBitmap, db string, frame string, bitmap_id, filter uint64, slice int, finish chan error) {
	compressed_bitmap := bitmap.ToCompressString()
	results := service.Batch(db, frame, compressed_bitmap, bitmap_id, slice, filter)
	finish <- results
}

func FromApiString(service *Service, db string, frame string, api_string string, bitmap_id, filter uint64) string {
	compressed_data, err := base64.StdEncoding.DecodeString(api_string)
	if err != nil {
		log.Warn(err)
		return "Bad"
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed_data))
	if err != nil {
		log.Warn(err)
		return "Bad"
	}
	var numChunks uint64
	err = binary.Read(reader, binary.BigEndian, &numChunks)
	if err != nil {
		log.Warn(err)
		return "Bad"
	}
	first := true
	bitmap := index.NewBitmap()
	last_slice := index.COUNTERMASK
	sent_count := 0
	finish := make(chan error)

	for i := uint64(0); i < numChunks; i++ {
		var raw struct {
			Key   uint64
			Block [32]uint64
		}
		binary.Read(reader, binary.BigEndian, &raw)
		slice := raw.Key >> 5
		if slice != last_slice {
			if first {
				first = false
			} else {
				//make async latter
				sent_count += 1
				go sendBitmap(service, bitmap, db, frame, bitmap_id, filter, int(last_slice), finish)
				bitmap = index.NewBitmap()
			}
			last_slice = slice
		}
		o := copy_raw(raw.Block)
		chunk := &index.Chunk{raw.Key, o}
		println("KEY", raw.Key)
		bitmap.AddChunk(chunk)

	}
	sent_count += 1
	go sendBitmap(service, bitmap, db, frame, bitmap_id, filter, int(last_slice), finish)
	for i := 0; i < sent_count; i++ {
		<-finish
	}
	return "OK"

}
