package core

import (
	"log"
	"pilosa/db"
	"pilosa/index"
	"pilosa/query"
	"pilosa/util"
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

func (self *Service) DifferenceQueryStepHandler(msg *db.Message) {
	//spew.Dump("DIFFERENCE QUERYSTEP")
	qs := msg.Data.(query.DifferenceQueryStep)
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

	bh, err := self.Index.Difference(qs.Location.FragmentId, handles)
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
	result_message := db.Message{Data: query.DifferenceQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: result}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Service) StashQueryStepHandler(msg *db.Message) {
	qs := msg.Data.(query.StashQueryStep)

	part := make(chan interface{})
	num_parts := len(qs.Inputs)

	for _, input := range qs.Inputs {
		go func(id *util.GUID, part chan interface{}) {
			value, _ := self.Hold.Get(id, 10)
			part <- value
		}(input, part)
	}
	//just collect all the handles and return them
	result := query.Stash{make([]query.CacheItem, 0)}
	for i := 0; i < num_parts; i++ {
		value := <-part

		switch val := value.(type) {
		case index.BitmapHandle:
			log.Println("STASH ADDING HANDLE", val)
			//not sure what to do here....
			//result.Handles = append(result.Handles, val)
		case []byte:
			bh, _ := self.Index.FromBytes(qs.Location.FragmentId, val)
			item := query.CacheItem{qs.Location.FragmentId, bh}
			result.Stash = append(result.Stash, item)
		case query.Stash:
			result.Stash = append(result.Stash, val.Stash...)
		default:
			log.Println("UNEXCPECTED MESSAG", value)
		}
	}
	result_message := db.Message{Data: query.StashQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: result}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)

}

func (self *Service) CatQueryStepHandler(msg *db.Message) {
	qs := msg.Data.(query.CatQueryStep)
	spew.Dump("CAT QUERYSTEP")
	var handles []index.BitmapHandle
	return_type := "bitmap-handles"
	var sum uint64
	merge_map := make(map[uint64]uint64)
	slice_map := make(map[uint64]map[util.SUUID]struct{})
	all_slice := make(map[util.SUUID]struct {
		process util.GUID
		handle  index.BitmapHandle
	})

	// either create a list of bitmap handles to cat (i.e. union), or sum the integer values
	part := make(chan interface{})
	num_parts := len(qs.Inputs)

	for _, input := range qs.Inputs {
		go func(id *util.GUID, part chan interface{}) {
			value, _ := self.Hold.Get(id, 10)
			part <- value
		}(input, part)
	}

	//for _, input := range qs.Inputs {
	check_pair := false
	for i := 0; i < num_parts; i++ {
		value := <-part

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
		case TopNPackage:
			return_type = "pair-list"
			var e struct{}
			for _, pair := range val.Pairs {
				//merge_map[pair.Key] += pair.Count
				merge_map[pair.Key] += pair.Count
				mm, ok := slice_map[pair.Key]
				if !ok {
					mm = make(map[util.SUUID]struct{})
					slice_map[pair.Key] = mm
				}
				mm[val.FragmentId] = e
			}
			all_slice[val.FragmentId] = struct {
				process util.GUID
				handle  index.BitmapHandle
			}{val.ProcessId, val.HBitmap}
			check_pair = true
		}
	}
	if check_pair { //no point in doing this for non top-n handling
		tasks := BuildTask(merge_map, slice_map, all_slice)
		FetchMissing(tasks, self)
		for k, v := range GatherResults(tasks, self) {
			merge_map[k] += v
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
		sort.Sort(rank_list) // kinda seems like this copy is wasteful..i'll ponder
		items_size := min(len(merge_map), qs.N)
		pair_list := make([]index.Pair, 0, items_size+1)
		for i, r := range rank_list {
			if i < items_size {
				pair_list = append(pair_list, *r.Pair)
			} else {
				break
			}
		}
		result = pair_list
	} else {
		result = "NONE"
	}
	result_message := db.Message{Data: query.CatQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: result}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

func (self *Service) RangeQueryStepHandler(msg *db.Message) {
	qs := msg.Data.(query.RangeQueryStep)
	//spew.Dump("RANDE QUERYSTEP")

	bh, err := self.Index.Range(qs.Location.FragmentId, qs.Bitmap.Id, qs.Start, qs.End)
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
	result_message := db.Message{Data: query.RangeQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: result}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}
