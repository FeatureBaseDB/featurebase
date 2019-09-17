// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package roaring

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/pkg/errors"
)

// UnmarshalBinary decodes b from a binary-encoded byte slice. data can be in
// either official roaring format or Pilosa's roaring format.
func (b *Bitmap) UnmarshalBinary(data []byte) error {
	if data == nil {
		// Nothing to unmarshal
		return nil
	}
	statsHit("Bitmap/UnmarshalBinary")
	b.opN = 0 // reset opN since we're reading new data.
	fileMagic := uint32(binary.LittleEndian.Uint16(data[0:2]))
	if fileMagic == MagicNumber { // if pilosa roaring
		return errors.Wrap(b.unmarshalPilosaRoaring(data), "unmarshaling as pilosa roaring")
	}

	keyN, containerTyper, header, pos, haveRuns, err := readOfficialHeader(data)
	if err != nil {
		return errors.Wrap(err, "reading roaring header")
	}
	// Only the Pilosa roaring format has flags. The official Roaring format
	// hasn't got space in its header for flags.
	b.Flags = 0

	b.Containers.ResetN(int(keyN))
	// Descriptive header section: Read container keys and cardinalities.
	for i, buf := uint(0), data[header:]; i < uint(keyN); i, buf = i+1, buf[4:] {
		card := int(binary.LittleEndian.Uint16(buf[2:4])) + 1
		b.Containers.PutContainerValues(
			uint64(binary.LittleEndian.Uint16(buf[0:2])),
			containerTyper(i, card), /// container type voodo with isRunBitmap
			card,
			true)
	}

	// Read container offsets and attach data.
	if haveRuns {
		err := readWithRuns(b, data, pos, keyN)
		if err != nil {
			return errors.Wrap(err, "reading offsets from official roaring format")
		}
	} else {
		err := readOffsets(b, data, pos, keyN)
		if err != nil {
			return errors.Wrap(err, "reading official roaring format")
		}
	}
	return nil
}

func readOffsets(b *Bitmap, data []byte, pos int, keyN uint32) error {

	citer, _ := b.Containers.Iterator(0)
	for i, buf := 0, data[pos:]; i < int(keyN); i, buf = i+1, buf[4:] {
		// Verify the offset is fully formed
		if len(buf) < 4 {
			return fmt.Errorf("insufficient data for offsets: len=%d", len(buf))
		}
		offset := binary.LittleEndian.Uint32(buf[0:4])
		// Verify the offset is within the bounds of the input data.
		if int(offset) >= len(data) {
			return fmt.Errorf("offset out of bounds: off=%d, len=%d", offset, len(data))
		}

		// Map byte slice directly to the container data.
		citer.Next()
		_, c := citer.Value()
		switch c.typ() {
		case containerArray:
			c.setArray((*[0xFFFFFFF]uint16)(unsafe.Pointer(&data[offset]))[:c.N():c.N()])
		case containerBitmap:
			c.setBitmap((*[0xFFFFFFF]uint64)(unsafe.Pointer(&data[offset]))[:bitmapN:bitmapN])
		default:
			return fmt.Errorf("unsupported container type %d", c.typ())
		}
	}
	return nil
}

func readWithRuns(b *Bitmap, data []byte, pos int, keyN uint32) error {
	if len(data) < pos+runCountHeaderSize {
		return fmt.Errorf("insufficient data for offsets(run): len=%d", len(data))
	}
	citer, _ := b.Containers.Iterator(0)
	for i := 0; i < int(keyN); i++ {
		citer.Next()
		_, c := citer.Value()
		switch c.typ() {
		case containerRun:
			runCount := binary.LittleEndian.Uint16(data[pos : pos+runCountHeaderSize])
			c.setRuns((*[0xFFFFFFF]interval16)(unsafe.Pointer(&data[pos+runCountHeaderSize]))[:runCount:runCount])
			runs := c.runs()

			for o := range runs { // must convert from start:length to start:end :(
				runs[o].last = runs[o].start + runs[o].last
			}
			pos += int((runCount * interval16Size) + runCountHeaderSize)
		case containerArray:
			c.setArray((*[0xFFFFFFF]uint16)(unsafe.Pointer(&data[pos]))[:c.N():c.N()])
			pos += int(c.N() * 2)
		case containerBitmap:
			c.setBitmap((*[0xFFFFFFF]uint64)(unsafe.Pointer(&data[pos]))[:bitmapN:bitmapN])
			pos += bitmapN * 8
		}
	}
	return nil
}

func (b *Bitmap) unmarshalPilosaRoaring(data []byte) error {
	if len(data) < headerBaseSize {
		return errors.New("data too small")
	}

	// Verify the first two bytes are a valid MagicNumber, and second two bytes match current storageVersion.
	fileMagic := uint32(binary.LittleEndian.Uint16(data[0:2]))
	fileVersion := uint32(data[2])
	b.Flags = data[3]
	if fileMagic != MagicNumber {
		return fmt.Errorf("invalid roaring file, magic number %v is incorrect", fileMagic)
	}

	if fileVersion != storageVersion {
		return fmt.Errorf("wrong roaring version, file is v%d, server requires v%d", fileVersion, storageVersion)
	}

	// Read key count in bytes sizeof(cookie)+sizeof(flag):(sizeof(cookie)+sizeof(uint32)).
	keyN := binary.LittleEndian.Uint32(data[3+1 : 8])
	if uint32(len(data)) < headerBaseSize+keyN*12 {
		return fmt.Errorf("insufficient data for header + offsets: key-cardinality not provided for %d containers", int(keyN)/12)
	}

	headerSize := headerBaseSize
	b.Containers.ResetN(int(keyN))
	// Descriptive header section: Read container keys and cardinalities.
	for i, buf := 0, data[headerSize:]; i < int(keyN); i, buf = i+1, buf[12:] {
		b.Containers.PutContainerValues(
			binary.LittleEndian.Uint64(buf[0:8]),
			byte(binary.LittleEndian.Uint16(buf[8:10])),
			int(binary.LittleEndian.Uint16(buf[10:12]))+1,
			true)
	}
	opsOffset := headerSize + int(keyN)*12

	// Read container offsets and attach data.
	citer, _ := b.Containers.Iterator(0)
	for i, buf := 0, data[opsOffset:]; i < int(keyN); i, buf = i+1, buf[4:] {
		offset := binary.LittleEndian.Uint32(buf[0:4])
		// Verify the offset is within the bounds of the input data.
		if int(offset) >= len(data) {
			return fmt.Errorf("offset out of bounds: off=%d, len=%d", offset, len(data))
		}

		// Map byte slice directly to the container data.
		citer.Next()
		_, c := citer.Value()

		// this shouldn't happen, since we don't normally store nils.
		if c == nil {
			continue
		}
		switch c.typ() {
		case containerRun:
			runCount := binary.LittleEndian.Uint16(data[offset : offset+runCountHeaderSize])
			c.setRuns((*[0xFFFFFFF]interval16)(unsafe.Pointer(&data[offset+runCountHeaderSize]))[:runCount:runCount])
			opsOffset = int(offset) + runCountHeaderSize + len(c.runs())*interval16Size
		case containerArray:
			c.setArray((*[0xFFFFFFF]uint16)(unsafe.Pointer(&data[offset]))[:c.N():c.N()])
			opsOffset = int(offset) + len(c.array())*2 // sizeof(uint32)
		case containerBitmap:
			c.setBitmap((*[0xFFFFFFF]uint64)(unsafe.Pointer(&data[offset]))[:bitmapN:bitmapN])
			opsOffset = int(offset) + len(c.bitmap())*8 // sizeof(uint64)
		}
	}

	// Read ops log until the end of the file.
	buf := data[opsOffset:]

	for {
		// Exit when there are no more ops to parse.
		if len(buf) == 0 {
			break
		}
		// Unmarshal the op and apply it.
		var opr op
		if err := opr.UnmarshalBinary(buf); err != nil {
			// FIXME(benbjohnson): return error with position so file can be trimmed.
			return err
		}
		opr.apply(b)
		// Increase the op count.
		b.ops++
		b.opN += opr.count()
		// Move the buffer forward.
		buf = buf[opr.size():]
	}

	return nil
}
