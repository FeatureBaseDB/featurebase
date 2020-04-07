// Copyright 2019 Pilosa Corp.
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
	"errors"
	"io"
)

// UnmarshalBinary reads Pilosa's format, or upstream roaring (mostly;
// it may not handle some edge cases), and decodes them into the given
// bitmap, replacing the existing contents.
func (b *Bitmap) UnmarshalBinary(data []byte) (err error) {
	if data == nil {
		return errors.New("no roaring bitmap provided")
	}
	var itr roaringIterator
	var itrKey uint64
	var itrCType byte
	var itrN int
	var itrLen int
	var itrPointer *uint16
	var itrErr error

	itr, err = newRoaringIterator(data)
	if err != nil {
		return err
	}
	if itr == nil {
		return errors.New("failed to create roaring iterator, but don't know why")
	}

	b.Containers.Reset()

	itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	for itrErr == nil {
		newC := &Container{
			typeID:  itrCType,
			n:       int32(itrN),
			len:     int32(itrLen),
			cap:     int32(itrLen),
			pointer: itrPointer,
			flags:   flagMapped,
		}
		if !b.preferMapping {
			newC.unmapOrClone()
		}
		b.Containers.Put(itrKey, newC)
		itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	}
	// note: if we get a non-EOF err, it's possible that we made SOME
	// changes but didn't log them. I don't have a good solution to this.
	if itrErr != io.EOF {
		return itrErr
	}

	// Read ops log until the end of the file.
	b.ops = 0
	b.opN = 0
	buf, lastValidOffset := itr.Remaining()
	for {
		// Exit when there are no more ops to parse.
		if len(buf) == 0 {
			break
		}

		// Unmarshal the op and apply it.
		var opr op
		if err := opr.UnmarshalBinary(buf); err != nil {
			return newFileShouldBeTruncatedError(err, int64(lastValidOffset))
		}

		opr.apply(b)

		// Increase the op count.
		b.ops++
		b.opN += opr.count()

		// Move the buffer forward.
		opSize := opr.size()
		buf = buf[opSize:]
		lastValidOffset += int64(opSize)
	}
	return nil
}

// InspectBinary reads a roaring bitmap, plus a possible ops log,
// and reports back on the contents, including distinguishing between
// the original ops log and the post-ops-log contents.
func InspectBinary(data []byte) (info BitmapInfo, err error) {
	if data == nil {
		return info, errors.New("no roaring bitmap provided")
	}
	var itr roaringIterator
	var itrKey uint64
	var itrCType byte
	var itrN int
	var itrLen int
	var itrPointer *uint16
	var itrErr error

	itr, err = newRoaringIterator(data)
	if err != nil {
		return info, err
	}
	if itr == nil {
		return info, errors.New("failed to create roaring iterator, but don't know why")
	}

	b := NewFileBitmap()

	itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	for itrErr == nil {
		newC := &Container{
			typeID:  itrCType,
			n:       int32(itrN),
			len:     int32(itrLen),
			cap:     int32(itrLen),
			pointer: itrPointer,
			flags:   flagMapped,
		}
		b.Containers.Put(itrKey, newC)
		itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	}
	// note: if we get a non-EOF err, it's possible that we made SOME
	// changes but didn't log them. I don't have a good solution to this.
	if itrErr != io.EOF {
		return info, itrErr
	}

	// gather initial info
	info = b.Info()

	// Read ops log until the end of the file.
	b.ops = 0
	b.opN = 0
	buf, lastValidOffset := itr.Remaining()
	for {
		// Exit when there are no more ops to parse.
		if len(buf) == 0 {
			break
		}

		// Unmarshal the op and apply it.
		var opr op
		if err := opr.UnmarshalBinary(buf); err != nil {
			return info, err
		}
		opr.apply(b)

		// Increase the op count.
		info.Ops++
		info.OpN += opr.count()
		info.OpDetails = append(info.OpDetails, opr.info())

		// Move the buffer forward.
		opSize := opr.size()
		buf = buf[opSize:]
		lastValidOffset += int64(opSize)
	}
	// now we want to compute the actual container and bit counts after
	// ops, and create a report of just the containers which got changed.
	info.ContainerCount = 0
	info.BitCount = 0
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		k, c := citer.Value()
		info.ContainerCount++
		info.BitCount += uint64(c.N())
		if c.Mapped() {
			continue
		}
		ci := c.info()
		ci.Key = k
		info.OpContainers = append(info.OpContainers, ci)
	}
	return info, nil
}
