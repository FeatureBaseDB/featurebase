package core

import (
	"pilosa/db"
	"pilosa/index"
	"pilosa/query"
	"sort"

	"github.com/davecgh/go-spew/spew"
)

func (self *Service) CountQueryStepHandler(msg *db.Message) {
	//spew.Dump("COUNT QUERYSTEP")
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
	//spew.Dump("SLICE COUNT", count)
	result_message := db.Message{Data: query.CountQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: count}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Service) TopNQueryStepHandler(msg *db.Message) {
	//spew.Dump("TOP-N QUERYSTEP")
	qs := msg.Data.(query.TopNQueryStep)
	//spew.Dump(qs)
	//TRAVIS SEE HERE
	//need categories in qs I just added so it would compile
	var categoryleaves []uint64
	input := qs.Input
	value, _ := self.Hold.Get(input, 10)
	var bh index.BitmapHandle
	switch val := value.(type) {
	case index.BitmapHandle:
		bh = val
	case []byte:
		bh, _ = self.Index.FromBytes(qs.Location.FragmentId, val)
	}

	categoryleaves = qs.Filters

	/*
		spew.Dump(bh, qs.N*2, categoryleaves)
		result_message := db.Message{Data: "foobar"}
		self.Transport.Send(&result_message, qs.Destination.ProcessId)
	*/

	topn, err := self.Index.TopN(qs.Location.FragmentId, bh, qs.N*2, categoryleaves)
	if err != nil {
		spew.Dump(err)
	}
	result_message := db.Message{Data: query.TopNQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: topn}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)

}

func (self *Service) UnionQueryStepHandler(msg *db.Message) {
	//spew.Dump("UNION QUERYSTEP")
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
	//spew.Dump("INTERSECT QUERYSTEP")
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
	//spew.Dump("CAT QUERYSTEP")
	var handles []index.BitmapHandle
	return_type := "bitmap-handles"
	var sum uint64
	merge_map := map[uint64]uint64{}
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
			//spew.Dump(val)
			return_type = "sum"
			sum += val
		case []index.Pair:
			//spew.Dump(val)
			return_type = "pair-list"
			for _, pair := range val {
				_, ok := merge_map[pair.Key]
				if ok {
					merge_map[pair.Key] += pair.Count
				} else {
					merge_map[pair.Key] = pair.Count
				}
			}
		}
	}

	// either return the sum, or return the compressed bitmap resulting from the cat (union)
	var result interface{}
	if return_type == "sum" {
		result = sum
	} else if return_type == "bitmap-handles" {
		bh, err := self.Index.Union(qs.Location.FragmentId, handles)
		result, err = self.Index.GetBytes(qs.Location.FragmentId, bh)
		if err != nil {
			spew.Dump(err)
		}
	} else if return_type == "pair-list" {
		rank_list := make(index.RankList, 0, len(merge_map))
		for k, v := range merge_map {
			rank := new(index.Rank)
			rank.Pair = &index.Pair{k, v}
			rank_list = append(rank_list, rank)
		}
		sort.Sort(rank_list)
		pair_list := make([]index.Pair, 0, len(merge_map))
		for _, r := range rank_list {
			pair_list = append(pair_list, *r.Pair)
		}
		result = pair_list[:qs.N]
	} else {
		result = "NONE"
	}
	result_message := db.Message{Data: query.CatQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: result}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Service) GetQueryStepHandler(msg *db.Message) {
	qs := msg.Data.(query.GetQueryStep)
	//spew.Dump("GET QUERYSTEP")

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
	//spew.Dump("SET QUERYSTEP")
	qs := msg.Data.(query.SetQueryStep)
	result, _ := self.Index.SetBit(qs.Location.FragmentId, qs.Bitmap.Id, qs.ProfileId, qs.Bitmap.Filter)

	result_message := db.Message{Data: query.SetQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: result}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}
