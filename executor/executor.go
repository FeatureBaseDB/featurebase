package executor

import (
	"encoding/gob"
	"sort"
	"time"

	log "github.com/cihub/seelog"
	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/core"
	"github.com/umbel/pilosa/db"
	"github.com/umbel/pilosa/query"
)

const DefaultTimeout = 30 * time.Second

type Executor struct {
	inbox chan *db.Message

	ID          pilosa.GUID
	Cluster     *db.Cluster
	ProcessMap  *core.ProcessMap
	PluginsPath string

	Hold interface {
		Get(id *pilosa.GUID, timeout time.Duration) (interface{}, error)
		Set(id *pilosa.GUID, value interface{}, timeout time.Duration)
	}

	Index interface {
		ClearBit(frag_id pilosa.SUUID, bitmap_id uint64, pos uint64) (bool, error)
		Count(frag_id pilosa.SUUID, bitmap pilosa.BitmapHandle) (uint64, error)
		Difference(frag_id pilosa.SUUID, bh []pilosa.BitmapHandle) (pilosa.BitmapHandle, error)
		FromBytes(frag_id pilosa.SUUID, bytes []byte) (pilosa.BitmapHandle, error)
		Get(frag_id pilosa.SUUID, bitmap_id uint64) (pilosa.BitmapHandle, error)
		GetBytes(frag_id pilosa.SUUID, bh pilosa.BitmapHandle) ([]byte, error)
		Intersect(frag_id pilosa.SUUID, bh []pilosa.BitmapHandle) (pilosa.BitmapHandle, error)
		Range(frag_id pilosa.SUUID, bitmap_id uint64, start, end time.Time) (pilosa.BitmapHandle, error)
		SetBit(frag_id pilosa.SUUID, bitmap_id uint64, pos uint64, category uint64) (bool, error)
		TopN(frag_id pilosa.SUUID, bh pilosa.BitmapHandle, n int, categories []uint64) ([]pilosa.Pair, error)
		TopNAll(frag_id pilosa.SUUID, n int, categories []uint64) ([]pilosa.Pair, error)
		Union(frag_id pilosa.SUUID, bh []pilosa.BitmapHandle) (pilosa.BitmapHandle, error)
	}

	TopologyMapper interface {
		MakeFragments(db string, slice_int int) error
	}

	Transport interface {
		Send(*db.Message, *pilosa.GUID)
	}
}

func NewExecutor(id pilosa.GUID) *Executor {
	log.Trace("NewExector")
	return &Executor{inbox: make(chan *db.Message)}
}

func (self *Executor) Init() error {
	log.Trace("Executor.Init()")
	return nil
}

func (self *Executor) Close() {
	log.Trace("Executor.Close()")
}

func (self *Executor) NewJob(job *db.Message) {
	log.Trace("NewJob", job)
	switch job.Data.(type) {
	case query.CountQueryStep:
		self.CountQueryStepHandler(job)
	case query.TopNQueryStep:
		self.TopNQueryStepHandler(job)
	case query.UnionQueryStep:
		self.UnionQueryStepHandler(job)
	case query.IntersectQueryStep:
		self.IntersectQueryStepHandler(job)
	case query.DifferenceQueryStep:
		self.DifferenceQueryStepHandler(job)
	case query.CatQueryStep:
		self.CatQueryStepHandler(job)
	case query.GetQueryStep:
		self.GetQueryStepHandler(job)
	case query.SetQueryStep:
		self.SetQueryStepHandler(job)
	case query.ClearQueryStep:
		self.ClearQueryStepHandler(job)
	case query.RangeQueryStep:
		self.RangeQueryStepHandler(job)
	case query.StashQueryStep:
		self.StashQueryStepHandler(job)
	default:
		log.Warn("unknown")
		log.Warn(spew.Sdump(job.Data))
	}
}

func (self *Executor) CountQueryStepHandler(msg *db.Message) {
	log.Trace("CountQueryStepHandler")
	//spew.Dump("COUNT QUERYSTEP")
	qs := msg.Data.(query.CountQueryStep)
	input := qs.Input
	value, _ := self.Hold.Get(input, DefaultTimeout)
	var bh pilosa.BitmapHandle
	switch val := value.(type) {
	case pilosa.BitmapHandle:
		bh = val
	case []byte:
		bh, _ = self.Index.FromBytes(qs.Location.FragmentId, val)
	}
	count, err := self.Index.Count(qs.Location.FragmentId, bh)
	if err != nil {
		spew.Dump(err)
	}
	//spew.Dump("SLICE COUNT", count)
	result_message := db.Message{
		Data: query.CountQueryResult{
			BaseQueryResult: &query.BaseQueryResult{Id: qs.Id, Data: count},
		},
	}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Executor) TopNQueryStepHandler(msg *db.Message) {
	qs := msg.Data.(query.TopNQueryStep)
	var bh pilosa.BitmapHandle
	var topnPackage TopNPackage

	// if we have an input, hold for it. if we don't, we assume an all() query
	if qs.Input == nil {
		topn, err := self.Index.TopNAll(qs.Location.FragmentId, qs.N*2, qs.Filters)
		if err != nil {
			log.Warn(spew.Sdump(err))
		}
		topnPackage = TopNPackage{*qs.Location.ProcessId, qs.Location.FragmentId, topn, bh}
	} else {
		input := qs.Input
		value, _ := self.Hold.Get(input, 10)
		//var bh pilosa.BitmapHandle
		switch val := value.(type) {
		case pilosa.BitmapHandle:
			bh = val
		case []byte:
			bh, _ = self.Index.FromBytes(qs.Location.FragmentId, val)
		}

		topn, err := self.Index.TopN(qs.Location.FragmentId, bh, qs.N*2, qs.Filters)
		if err != nil {
			log.Warn(spew.Sdump(err))
		}
		topnPackage = TopNPackage{*qs.Location.ProcessId, qs.Location.FragmentId, topn, bh}
	}

	result_message := db.Message{
		Data: query.TopNQueryResult{
			BaseQueryResult: &query.BaseQueryResult{Id: qs.Id, Data: topnPackage},
		},
	}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Executor) UnionQueryStepHandler(msg *db.Message) {
	log.Trace("UnionQueryStepHandler")
	//spew.Dump("UNION QUERYSTEP")

	qs := msg.Data.(query.UnionQueryStep)
	var handles []pilosa.BitmapHandle
	// create a list of bitmap handles
	for _, input := range qs.Inputs {
		value, _ := self.Hold.Get(input, DefaultTimeout)
		switch val := value.(type) {
		case pilosa.BitmapHandle:
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
	result_message := db.Message{
		Data: query.UnionQueryResult{
			BaseQueryResult: &query.BaseQueryResult{Id: qs.Id, Data: result},
		},
	}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Executor) IntersectQueryStepHandler(msg *db.Message) {
	log.Trace("IntersectQueryStepHandler")
	//spew.Dump("INTERSECT QUERYSTEP")
	qs := msg.Data.(query.IntersectQueryStep)
	var handles []pilosa.BitmapHandle
	// create a list of bitmap handles
	for _, input := range qs.Inputs {
		value, _ := self.Hold.Get(input, DefaultTimeout)
		switch val := value.(type) {
		case pilosa.BitmapHandle:
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
	result_message := db.Message{
		Data: query.IntersectQueryResult{
			BaseQueryResult: &query.BaseQueryResult{Id: qs.Id, Data: result},
		},
	}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Executor) DifferenceQueryStepHandler(msg *db.Message) {
	log.Trace("DifferenceQueryStepHandler")
	//spew.Dump("DIFFERENCE QUERYSTEP")
	qs := msg.Data.(query.DifferenceQueryStep)
	var handles []pilosa.BitmapHandle
	// create a list of bitmap handles
	for _, input := range qs.Inputs {
		value, _ := self.Hold.Get(input, DefaultTimeout)
		switch val := value.(type) {
		case pilosa.BitmapHandle:
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
	result_message := db.Message{
		Data: query.DifferenceQueryResult{
			BaseQueryResult: &query.BaseQueryResult{Id: qs.Id, Data: result},
		},
	}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Executor) CatQueryStepHandler(msg *db.Message) {
	log.Trace("CatQueryStepHandler")
	qs := msg.Data.(query.CatQueryStep)
	var handles []pilosa.BitmapHandle
	return_type := "bitmap-handles"
	var sum uint64
	merge_map := make(map[uint64]uint64)
	slice_map := make(map[uint64]map[pilosa.SUUID]struct{})
	all_slice := make(map[pilosa.SUUID]struct {
		process pilosa.GUID
		handle  pilosa.BitmapHandle
	})

	// either create a list of bitmap handles to cat (i.e. union), or sum the integer values
	part := make(chan interface{})
	num_parts := len(qs.Inputs)

	for _, input := range qs.Inputs {
		go func(id *pilosa.GUID, part chan interface{}) {
			value, _ := self.Hold.Get(id, DefaultTimeout)
			part <- value
		}(input, part)
	}

	//for _, input := range qs.Inputs {
	check_pair := false
	for i := 0; i < num_parts; i++ {
		value := <-part

		switch val := value.(type) {
		case pilosa.BitmapHandle:
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
				if pair.Key == 0 {
					continue //skip
				}
				merge_map[pair.Key] += pair.Count
				mm, ok := slice_map[pair.Key]
				if !ok {
					mm = make(map[pilosa.SUUID]struct{})
					slice_map[pair.Key] = mm
				}
				mm[val.FragmentId] = e
			}
			all_slice[val.FragmentId] = struct {
				process pilosa.GUID
				handle  pilosa.BitmapHandle
			}{val.ProcessId, val.HBitmap}
			check_pair = true
		}
	}
	if check_pair { //no point in doing this for non top-n handling
		tasks := BuildTask(merge_map, slice_map, all_slice)
		self.FetchMissing(tasks)
		for k, v := range self.GatherResults(tasks) {
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
		rank_list := make(pilosa.RankList, 0, len(merge_map))
		for k, v := range merge_map {
			if k == 0 || v == 0 {
				continue //shouldn't be getting 0 keys or values anyway
			}
			rank := new(pilosa.Rank)
			rank.Pair = &pilosa.Pair{Key: k, Count: v}
			rank_list = append(rank_list, rank)
		}
		sort.Sort(rank_list) // kinda seems like this copy is wasteful..i'll ponder
		items_size := min(len(merge_map), qs.N)
		pair_list := make([]pilosa.Pair, 0, items_size+1)
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
	result_message := db.Message{
		Data: query.CatQueryResult{
			BaseQueryResult: &query.BaseQueryResult{Id: qs.Id, Data: result},
		},
	}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Executor) SendRequest(process_id pilosa.GUID, t *Task) {
	args := make([]pilosa.FillArgs, len(t.f), len(t.f))
	for _, v := range t.f {
		args = append(args, v)
	}
	msg := new(db.Message)
	p, _ := self.ProcessMap.GetProcess(&self.ID)
	msg.Data = TopFill{args, p.Id(), t.hold_id, process_id}
	self.Transport.Send(msg, &process_id)
}

func (self *Executor) FetchMissing(tasks map[pilosa.GUID]*Task) {
	for k, v := range tasks {
		go self.SendRequest(k, v)
	}
}

func (self *Executor) GatherResults(tasks map[pilosa.GUID]*Task) map[uint64]uint64 {
	results := make(map[uint64]uint64)
	answers := make(chan []pilosa.Pair)
	for _, task := range tasks {
		go func(id pilosa.GUID) {
			value, err := self.Hold.Get(&id, 10) //eiher need to be the frame process or the handler process?
			if value == nil {
				log.Warn("Bad TopN Result:", err)
				empty := make([]pilosa.Pair, 0, 0)
				answers <- empty

			} else {
				answers <- value.([]pilosa.Pair)
			}
		}(task.hold_id)
	}
	for i := 0; i < len(tasks); i++ {
		batch := <-answers
		for _, pair := range batch {
			results[pair.Key] += pair.Count
		}
	}
	close(answers)
	return results
}

func (self *Executor) GetQueryStepHandler(msg *db.Message) {
	qs := msg.Data.(query.GetQueryStep)
	//spew.Dump("GET QUERYSTEP")

	bh, err := self.Index.Get(qs.Location.FragmentId, qs.Bitmap.Id)
	if err != nil {
		spew.Dump(err)
		log.Error("GetQueryStepHandler1", qs.Location.FragmentId.String(), qs.Bitmap.Id)
		log.Error("GetQueryStepHandler2", err)
	}

	var result interface{}
	if qs.LocIsDest() {
		result = bh
	} else {
		bm, err := self.Index.GetBytes(qs.Location.FragmentId, bh)
		if err != nil {
			spew.Dump(err)
			log.Error("GetQueryStepHandlerr3", qs.Location.FragmentId.String(), qs.Bitmap.Id)
			log.Error("GetQueryStepHandler4", err)
		}
		result = bm
	}
	result_message := db.Message{
		Data: query.GetQueryResult{
			BaseQueryResult: &query.BaseQueryResult{Id: qs.Id, Data: result},
		},
	}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Executor) SetQueryStepHandler(msg *db.Message) {
	//spew.Dump("SET QUERYSTEP")
	qs := msg.Data.(query.SetQueryStep)
	result, _ := self.Index.SetBit(qs.Location.FragmentId, qs.Bitmap.Id, qs.ProfileId, qs.Bitmap.Filter)

	result_message := db.Message{
		Data: query.SetQueryResult{
			BaseQueryResult: &query.BaseQueryResult{Id: qs.Id, Data: result},
		},
	}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Executor) ClearQueryStepHandler(msg *db.Message) {
	//spew.Dump("SET QUERYSTEP")
	qs := msg.Data.(query.ClearQueryStep)
	result, _ := self.Index.ClearBit(qs.Location.FragmentId, qs.Bitmap.Id, qs.ProfileId)

	result_message := db.Message{
		Data: query.ClearQueryResult{
			BaseQueryResult: &query.BaseQueryResult{Id: qs.Id, Data: result},
		},
	}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Executor) RangeQueryStepHandler(msg *db.Message) {
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
	result_message := db.Message{
		Data: query.RangeQueryResult{
			BaseQueryResult: &query.BaseQueryResult{Id: qs.Id, Data: result},
		},
	}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func (self *Executor) StashQueryStepHandler(msg *db.Message) {
	log.Trace("StashQueryStepHandler")
	qs := msg.Data.(query.StashQueryStep)

	part := make(chan interface{})
	num_parts := len(qs.Inputs)

	for _, input := range qs.Inputs {
		go func(id *pilosa.GUID, part chan interface{}) {
			value, _ := self.Hold.Get(id, DefaultTimeout)
			part <- value
		}(input, part)
	}
	//just collect all the handles and return them
	result := query.NewStash() //query.Stash{make([]query.CacheItem, 0), false}
	for i := 0; i < num_parts; i++ {
		value := <-part

		switch val := value.(type) {
		case pilosa.BitmapHandle:
			log.Info("STASH ADDING HANDLE", val)
			//not sure what to do here....
			//result.Handles = append(result.Handles, val)
		case []byte:
			bh, _ := self.Index.FromBytes(qs.Location.FragmentId, val)
			item := query.CacheItem{FragmentId: qs.Location.FragmentId, Handle: bh}
			result.Stash = append(result.Stash, item)
		case query.Stash:
			result.Stash = append(result.Stash, val.Stash...)
		default:
			log.Warn("UNEXCPECTED MESSAGE", value)
		}
	}
	result_message := db.Message{
		Data: query.StashQueryResult{
			BaseQueryResult: &query.BaseQueryResult{Id: qs.Id, Data: result},
		},
	}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)

}

func (self *Executor) RunQueryTest(database_name string, pql string) string {
	return pql
}

func (self *Executor) runQuery(database *db.Database, qry *query.Query) error {
	log.Trace("Executor.runQuery", database, qry)
	process, err := self.ProcessMap.GetProcess(&self.ID)
	if err != nil {
		return err
	}
	process_id := process.Id()
	fragment_id := pilosa.SUUID(0)
	destination := db.Location{ProcessId: &process_id, FragmentId: fragment_id}

	query_plan, err := query.QueryPlanForQuery(database, qry, &destination)
	if err != nil {
		switch obj := err.(type) {
		case *query.FragmentNotFound:
			self.TopologyMapper.MakeFragments(obj.Db, obj.Slice)
		}
		self.Hold.Set(qry.Id, err, 30)
		return err
	}
	// loop over the query steps and send to Transport
	for _, qs := range *query_plan {
		msg := new(db.Message)
		msg.Data = qs
		switch step := qs.(type) {
		case query.PortableQueryStep:
			loc := step.GetLocation()
			if loc != nil {
				self.Transport.Send(msg, loc.ProcessId)
			} else {
				log.Warn("Problem with querystep(nil location)", spew.Sdump(step))
			}
		}
	}
	return nil
}

func (self *Executor) RunPQL(database_name string, pql string) (interface{}, error) {
	log.Trace("Executor.RunPQL", database_name, pql)
	database := self.Cluster.GetOrCreateDatabase(database_name)

	// see if the outer query function is a custom query
	reserved_functions := stringSlice{"get", "set", "clear", "union", "intersect", "difference", "count", "top-n", "mask", "range", "stash", "recall"}
	tokens, err := query.Lex(pql)
	if err != nil {
		return nil, err
	}
	outer_token := tokens[0].Text

	if reserved_functions.pos(outer_token) != -1 {

		qry, err := query.QueryForTokens(tokens)
		if err != nil {
			return nil, err
		}
		go self.runQuery(database, qry)

		var final interface{}
		final, err = self.Hold.Get(qry.Id, 10)
		if err != nil {
			return nil, err
		}
		return final, nil

	} else { //want to refactor this down to just RunPlugin(tokens)
		plugins_file := self.PluginsPath + "/" + outer_token + ".js"
		filter, filters := query.TokensToFilterStrings(tokens)
		query_list := GetPlugin(plugins_file, filter, filters).(query.PqlList)

		for i, _ := range query_list {
			qry, err := query.QueryForPQL(query_list[i].PQL)
			if err != nil {
				return nil, err
			}
			go self.runQuery(database, qry)
			query_list[i].Id = qry.Id
		}

		final_result := make(map[string]interface{})
		result := make(chan struct {
			final interface{}
			label string
			err   error
		})
		x := 0
		for i, _ := range query_list {
			x++
			go func(q query.PqlListItem, reply chan struct {
				final interface{}
				label string
				err   error
			}) {
				final, err := self.Hold.Get(q.Id, 10)
				result <- struct {
					final interface{}
					label string
					err   error
				}{final, q.Label, err}
			}(query_list[i], result)
			if err != nil {
				out := spew.Sdump(err)
				log.Warn(out)
			}
		}
		for z := 0; z < x; z++ {
			ans := <-result
			final_result[ans.label] = ans.final
		}

		return final_result, nil
	}

}

func init() {
	gob.Register(TopNPackage{})
	gob.Register(TopFill{})
}

type TopNPackage struct {
	ProcessId  pilosa.GUID
	FragmentId pilosa.SUUID
	Pairs      []pilosa.Pair
	HBitmap    pilosa.BitmapHandle
}

type TopFill struct {
	Args            []pilosa.FillArgs
	ReturnProcessId pilosa.GUID
	QueryId         pilosa.GUID
	DestProcessId   pilosa.GUID
}

type Task struct {
	processid pilosa.GUID
	f         map[pilosa.SUUID]pilosa.FillArgs
	hold_id   pilosa.GUID
}

func newtask(p pilosa.GUID) *Task {
	result := new(Task)
	result.processid = p
	result.f = make(map[pilosa.SUUID]pilosa.FillArgs)
	result.hold_id = pilosa.NewGUID()
	return result
}

func (t *Task) Add(frag pilosa.SUUID, bitmap_id uint64, handle pilosa.BitmapHandle) {
	fa, ok := t.f[frag]
	if !ok {
		fa = pilosa.FillArgs{Frag_id: frag, Handle: handle, Bitmaps: make([]uint64, 0, 0)}
	}
	fa.Bitmaps = append(fa.Bitmaps, bitmap_id)
	t.f[frag] = fa
}

func BuildTask(merge_map map[uint64]uint64,
	slice_map map[uint64]map[pilosa.SUUID]struct{},
	total_fragments map[pilosa.SUUID]struct {
		process pilosa.GUID
		handle  pilosa.BitmapHandle
	}) map[pilosa.GUID]*Task {

	tasks := make(map[pilosa.GUID]*Task)
	for bitmap_id, _ := range merge_map { //for all brands
		//for fragment_id, reported_fragments := range slice_map[bitmap_id] { //find missing fragments
		reporting_fragments := slice_map[bitmap_id]
		//id slice ==> SUUID,BitmapHandle
		for _, p := range missing(reporting_fragments, total_fragments) {
			task, ok := tasks[p.process]
			if !ok {
				task = newtask(p.process)
				tasks[p.process] = task
			}
			task.Add(p.fragment, bitmap_id, p.handle)

		}
		//}
	}

	return tasks
}

type hole struct {
	process  pilosa.GUID
	handle   pilosa.BitmapHandle
	fragment pilosa.SUUID
}

func missing(fids map[pilosa.SUUID]struct{}, all map[pilosa.SUUID]struct {
	process pilosa.GUID
	handle  pilosa.BitmapHandle
}) []hole {
	results := make([]hole, 0, 0)

	for k, v := range all {
		_, ok := fids[k]
		if !ok {
			results = append(results, hole{v.process, v.handle, k})
		}
	}
	return results
}

func (self *TopFill) GetId() *pilosa.GUID {
	return &self.QueryId
}
func (self *TopFill) GetLocation() *db.Location {
	return &db.Location{ProcessId: &self.DestProcessId, FragmentId: 0} //this message is a broadcast to many fragments so i'm choosing fragmentzero
}

func (self *Executor) Run() {
	log.Warn("Executor Run...")
}

type stringSlice []string

func (slice stringSlice) pos(value string) int {
	for p, v := range slice {
		if v == value {
			return p
		}
	}
	return -1
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
