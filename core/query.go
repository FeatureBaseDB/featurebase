package core

import (
	"pilosa/db"
	"pilosa/index"
	"pilosa/query"

	"github.com/davecgh/go-spew/spew"
)

func (self *Service) CountQueryStepHandler(msg *db.Message) {
	spew.Dump("COUNT QUERYSTEP")
	qs := msg.Data.(query.CountQueryStep)
	input := qs.Input
	value, _ := self.Hold.Get(input, 10)
	var bh index.BitmapHandle
	switch val := value.(type) {
	case index.BitmapHandle:
		bh = val
	case []byte:
		bh, _ = self.Index.FromBytes(qs.Location.FragmentId, val)
	}
	count, err := self.Index.Count(qs.Location.FragmentId, bh)
	if err != nil {
		spew.Dump(err)
	}
	spew.Dump("SLICE COUNT", count)
	result_message := db.Message{Data: query.CountQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: count}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Service) UnionQueryStepHandler(msg *db.Message) {
	spew.Dump("UNION QUERYSTEP")
	qs := msg.Data.(query.UnionQueryStep)
	var handles []index.BitmapHandle
	// create a list of bitmap handles
	for _, input := range qs.Inputs {
		value, _ := self.Hold.Get(input, 10)
		switch val := value.(type) {
		case index.BitmapHandle:
			handles = append(handles, val)
		case []byte:
			bh, _ := self.Index.FromBytes(qs.Location.FragmentId, val)
			handles = append(handles, bh)
		}
	}

	bh, err := self.Index.Union(qs.Location.FragmentId, handles)
	if err != nil {
		spew.Dump(err)
	}

	var result interface{}
	if qs.LocIsDest() {
		result = bh
	} else {
		bm, err := self.Index.GetBytes(qs.Location.FragmentId, bh)
		if err != nil {
			spew.Dump(err)
		}
		result = bm
	}
	result_message := db.Message{Data: query.UnionQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: result}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Service) IntersectQueryStepHandler(msg *db.Message) {
	spew.Dump("INTERSECT QUERYSTEP")
	qs := msg.Data.(query.IntersectQueryStep)
	var handles []index.BitmapHandle
	// create a list of bitmap handles
	for _, input := range qs.Inputs {
		value, _ := self.Hold.Get(input, 10)
		switch val := value.(type) {
		case index.BitmapHandle:
			handles = append(handles, val)
		case []byte:
			bh, _ := self.Index.FromBytes(qs.Location.FragmentId, val)
			handles = append(handles, bh)
		}
	}

	bh, err := self.Index.Intersect(qs.Location.FragmentId, handles)
	if err != nil {
		spew.Dump(err)
	}

	var result interface{}
	if qs.LocIsDest() {
		result = bh
	} else {
		bm, err := self.Index.GetBytes(qs.Location.FragmentId, bh)
		if err != nil {
			spew.Dump(err)
		}
		result = bm
	}
	result_message := db.Message{Data: query.IntersectQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: result}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Service) CatQueryStepHandler(msg *db.Message) {
	qs := msg.Data.(query.CatQueryStep)
	spew.Dump("CAT QUERYSTEP")
	var handles []index.BitmapHandle
	use_sum := false
	var sum uint64
	// either create a list of bitmap handles to cat (i.e. union), or sum the integer values
	for _, input := range qs.Inputs {
		value, _ := self.Hold.Get(input, 10)
		switch val := value.(type) {
		case index.BitmapHandle:
			handles = append(handles, val)
		case []byte:
			bh, _ := self.Index.FromBytes(qs.Location.FragmentId, val)
			handles = append(handles, bh)
		case uint64:
			use_sum = true
			sum += value.(uint64)
		}
	}
	// either return the sum, or return the compressed bitmap resulting from the cat (union)
	var result interface{}
	if use_sum {
		result = sum
	} else {
		bh, err := self.Index.Union(qs.Location.FragmentId, handles)
		result, err = self.Index.GetBytes(qs.Location.FragmentId, bh)
		if err != nil {
			spew.Dump(err)
		}
	}
	result_message := db.Message{Data: query.CatQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: result}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Service) GetQueryStepHandler(msg *db.Message) {
	qs := msg.Data.(query.GetQueryStep)
	spew.Dump("GET QUERYSTEP")

	bh, err := self.Index.Get(qs.Location.FragmentId, qs.Bitmap.Id)
	if err != nil {
		spew.Dump(err)
	}

	var result interface{}
	if qs.LocIsDest() {
		result = bh
	} else {
		bm, err := self.Index.GetBytes(qs.Location.FragmentId, bh)
		if err != nil {
			spew.Dump(err)
		}
		result = bm
	}
	result_message := db.Message{Data: query.GetQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: result}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Service) SetQueryStepHandler(msg *db.Message) {
	spew.Dump("SET QUERYSTEP")
	qs := msg.Data.(query.SetQueryStep)
	self.Index.SetBit(qs.Location.FragmentId, qs.Bitmap.Id, qs.ProfileId)
}
