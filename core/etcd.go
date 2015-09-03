package core

import (
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	"github.com/coreos/go-etcd/etcd"
	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/db"
)

const (
	DefaultFragmentAllocLockTTL = 14400 * time.Second
)

type TopologyMapper struct {
	namespace string

	ID         pilosa.GUID
	Cluster    *db.Cluster
	ProcessMap *ProcessMap

	SupportedFrames      []string
	FragmentAllocLockTTL time.Duration

	EtcdClient interface {
		CreateDir(key string, ttl uint64) (*etcd.Response, error)
		Get(key string, sort, recursive bool) (*etcd.Response, error)
		RawCreate(key string, value string, ttl uint64) (*etcd.RawResponse, error)
		Set(key string, value string, ttl uint64) (*etcd.Response, error)
		Watch(prefix string, waitIndex uint64, recursive bool, receiver chan *etcd.Response, stop chan bool) (*etcd.Response, error)
	}

	Index interface {
		AddFragment(db string, frame string, slice int, id pilosa.SUUID)
	}
}

func NewTopologyMapper(namespace string) *TopologyMapper {
	return &TopologyMapper{
		namespace:            namespace,
		FragmentAllocLockTTL: DefaultFragmentAllocLockTTL,
	}
}

func (self *TopologyMapper) Setup() {
	log.Warn(self.namespace + "/db")
	db_path := self.namespace + "/db"
	resp, err := self.EtcdClient.Get(db_path, false, true)
	if err != nil {
		ee, ok := err.(*etcd.EtcdError)
		if ok && ee.ErrorCode == 100 { // node does not exist
			resp, err = self.EtcdClient.CreateDir(db_path, 0)
			if err != nil {
				log.Critical(err)
				os.Exit(-1)
			}
		} else {
			log.Critical(err)
			os.Exit(-1)
		}
	}

	//need to lock the world
	for _, node := range flatten(resp.Node) {
		err := self.handlenode(node)
		if err != nil {
			log.Warn(err)
		}
	}

}
func (self *TopologyMapper) Run() {

	receiver := make(chan *etcd.Response)
	go func() {
		// TODO: add some terminating measure
		// TODO: use modindex to make sure watch catches everything
		for {
			ns := self.namespace + "/db"
			log.Warn(" ETCD watcher:", ns)
			stop := make(chan bool)
			resp, err := self.EtcdClient.Watch(ns, 0, true, receiver, stop)
			log.Warn("TopologyMapper ETCD watcher", resp, err)
		}
	}()
	go func() {
		for resp := range receiver {
			switch resp.Action {
			case "set":
				self.handlenode(resp.Node)
			case "delete":
				self.remove_fragment(resp.Node)
			}
			// TODO: handle deletes
		}
	}()
}

type Pair struct {
	Key   string
	Value int
}

// A slice of Pairs that implements sort.Interface to sort by Value.
type PairList []Pair

func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }

func getLightestProcess(m map[string]int) (Pair, error) {

	l := len(m)
	if l == 0 {
		return Pair{}, errors.New("No Processes")
	}
	processlist := make(PairList, l)
	i := 0

	for k, v := range m {
		processlist[i] = Pair{k, v}
		i++
	}
	sort.Sort(processlist)

	return processlist[0], nil
}

func (self *TopologyMapper) GetProcessFragmentCounts() map[string]int {
	m := make(map[string]int)
	m[self.ID.String()] = 0 //at least have one process if none created
	for k, _ := range self.ProcessMap.nodes {
		p := k.String()
		m[p] = 0 //at least have one process if none created
	}

	for _, dbs := range self.Cluster.GetDatabases() {
		for _, fsi := range dbs.GetFrameSliceIntersects() {
			for _, fragment := range fsi.GetFragments() {
				process := fragment.GetProcess().Id().String()
				if len(process) > 1 {
					i := m[process]
					i++
					m[process] = i
				}
			}

		}

	}
	return m
}

func (self *TopologyMapper) MakeFragments(db string, slice_int int) error {
	lock_key := fmt.Sprintf("%s/lock/%s-%d", self.namespace, db, slice_int)
	response, err := self.EtcdClient.RawCreate(lock_key, "0", uint64(self.FragmentAllocLockTTL.Seconds()))

	if err == nil {
		if response.StatusCode == 201 { //key created
			log.Warn("MakeFragments:", db, slice_int)
			m := self.GetProcessFragmentCounts()
			p, err := getLightestProcess(m)
			if err != nil {
				log.Warn("MakeFragments: error finding process", db, slice_int, err)
				return err
			}

			// PUT -d "value=5cb315c3-6e1d-4218-89b7-943d1dba985b" http://etcd0:4001/v2/keys/pilosa/0/db/29/frame/d/slice/5/fragment/a2b632fc4001b817/proces
			for _, frame := range self.SupportedFrames {
				err := self.AllocateFragment(p.Key, db, frame, slice_int)
				if err != nil {
					log.Warn(err)
				}
			}
		}
	}
	return nil
}

func (self *TopologyMapper) AllocateFragment(process_guid, db, frame string, slice_int int) error {
	//get Lock to create the fragment
	//figure out least loaded process..possibly check max process
	//to create the node, just write off the items to etcd and the watch should spawn
	//be nice if something would notify perhaps queue
	//so i need db, frame, slice , fragment_id
	fuid := pilosa.NewSUUID().String()
	fragment_key := fmt.Sprintf("%s/db/%s/frame/%s/slice/%d/fragment/%s/process", self.namespace, db, frame, slice_int, fuid)
	// need to check value to see how many we have left
	log.Warn("ALLOC:", process_guid, len(process_guid))
	if len(process_guid) > 1 {
		_, err := self.EtcdClient.Set(fragment_key, process_guid, 0)
		if err != nil {
			return err
		}
		log.Warn("Fragment sent to etcd:", fragment_key, process_guid)
	}

	return nil

}

func (self *TopologyMapper) handlenode(node *etcd.Node) error {
	key := node.Key[len(self.namespace)+1:]
	bits := strings.Split(key, "/")
	var database *db.Database
	var frame *db.Frame
	var fragment *db.Fragment
	var fragment_id pilosa.SUUID
	var slice *db.Slice
	var slice_int int
	var process_uuid pilosa.GUID
	var process *db.Process
	var err error

	if len(bits) > 8 {
		if bits[8] != "process" {
			return errors.New("no process")
		}

		process_uuid, err = pilosa.ParseGUID(node.Value)
		if err != nil {
			log.Warn("Bad Process Guid", key)
			return errors.New("No Process Id")
		}
	} else {
		return err
	}

	if len(bits) <= 1 || bits[0] != "db" {
		return nil
	}
	if len(bits) > 1 {
		database = self.Cluster.GetOrCreateDatabase(bits[1])
	}
	if len(bits) > 2 {
		if bits[2] != "frame" {
			return errors.New("no frame")
		}
	}
	if len(bits) > 3 {
		frame = database.GetOrCreateFrame(bits[3])
	}
	if len(bits) > 4 {
		if bits[4] != "slice" {
			return errors.New("no slice")
		}
	}
	if len(bits) > 5 {
		slice_int, err = strconv.Atoi(bits[5])
		if err != nil {
			return err
		}
		slice = database.GetOrCreateSlice(slice_int)
	}
	if len(bits) > 6 {
		if bits[6] != "fragment" {
			return errors.New("no fragment")
		}
	}
	if len(bits) > 7 {
		fragment_id = pilosa.ParseSUUID(bits[7])
		fragment = database.GetOrCreateFragment(frame, slice, fragment_id)
	}

	if len(bits) > 8 {
		if bits[8] != "process" {
			return errors.New("no process")
		}

		if err != nil {
			log.Warn("Bad UUID:", process_uuid, key)
			return err
		}
		process = db.NewProcess(&process_uuid)
		fragment.SetProcess(process)

		if self.ID.Equals(&process_uuid) {
			self.Index.AddFragment(bits[1], bits[3], slice_int, fragment_id)
		}

	}
	return err
}
func (self *TopologyMapper) remove_fragment(node *etcd.Node) error {
	log.Warn(" hot remove_fragment (Not Supported yet):", node)
	return nil
}

func flatten(node *etcd.Node) []*etcd.Node {
	nodes := []*etcd.Node{node}
	for i := 0; i < len(node.Nodes); i++ {
		nodes = append(nodes, flatten(&node.Nodes[i])...)
	}
	return nodes
}

type Node struct {
	id        *pilosa.GUID
	ip        string
	port_tcp  int
	port_http int
}

type ProcessMap struct {
	nodes map[pilosa.GUID]*db.Process
	mutex sync.Mutex
}

func NewProcessMap() *ProcessMap {
	p := ProcessMap{}
	p.nodes = make(map[pilosa.GUID]*db.Process)
	return &p
}

func (self *ProcessMap) AddProcess(process *db.Process) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.nodes[process.Id()] = process
}

func (self *ProcessMap) GetProcess(id *pilosa.GUID) (*db.Process, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if id == nil {
		debug.PrintStack()
		return nil, errors.New("Nil process")
	}
	process, ok := self.nodes[*id]
	if !ok {
		return nil, errors.New("No such process")
	}
	return process, nil
}

func (self *ProcessMap) GetOrAddProcess(id *pilosa.GUID) *db.Process {
	process, err := self.GetProcess(id)
	if err != nil {
		process = db.NewProcess(id)
		self.AddProcess(process)
	}
	return process
}

func (self *ProcessMap) GetHost(id *pilosa.GUID) (string, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	process, ok := self.nodes[*id]
	if !ok {
		return "", errors.New("Process does not exist")
	}
	return process.Host(), nil
}

func (self *ProcessMap) GetPortTcp(id *pilosa.GUID) (int, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	process, ok := self.nodes[*id]
	if !ok {
		return 0, errors.New("Process does not exist")
	}
	return process.PortTcp(), nil
}

func (self *ProcessMap) GetPortHttp(id *pilosa.GUID) (int, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	process, ok := self.nodes[*id]
	if !ok {
		return 0, errors.New("Process does not exist")
	}
	return process.PortHttp(), nil
}

func (self *ProcessMap) GetMetadata() map[string]map[string]interface{} {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	out := make(map[string]map[string]interface{})
	for id, process := range self.nodes {
		pdata := make(map[string]interface{})
		pdata["host"] = process.Host()
		pdata["port_tcp"] = process.PortTcp()
		pdata["port_http"] = process.PortHttp()
		out[id.String()] = pdata
	}
	return out
}

type ProcessMapper struct {
	receiver  chan etcd.Response
	commands  chan ProcessMapperCommand
	namespace string

	ID         pilosa.GUID
	ProcessMap *ProcessMap

	TCPPort  int
	HTTPPort int
	Host     string

	EtcdClient interface {
		Get(key string, sort, recursive bool) (*etcd.Response, error)
		Set(key string, value string, ttl uint64) (*etcd.Response, error)
		Watch(prefix string, waitIndex uint64, recursive bool, receiver chan *etcd.Response, stop chan bool) (*etcd.Response, error)
	}
}

func NewProcessMapper(namespace string) *ProcessMapper {
	return &ProcessMapper{
		receiver:  make(chan etcd.Response),
		commands:  make(chan ProcessMapperCommand),
		namespace: namespace,
	}
}

type ProcessMapperCommand struct {
	key string
}

func getKey(input string) string {
	bits := strings.Split(input, "/")
	return bits[len(bits)-1]
}

func (self *ProcessMapper) getnode(u *pilosa.GUID) *Node {
	return new(Node)
}

func crash_on_error(err error) {
	if err != nil {
		log.Critical(err)
		os.Exit(-1)
	}
}

func (self *ProcessMapper) handlenode(node *etcd.Node) error {
	var err error
	var process *db.Process
	key := node.Key[len(self.namespace)+1:]
	bits := strings.Split(key, "/")
	if len(bits) <= 1 || bits[0] != "process" {
		return nil
	}
	if len(bits) >= 2 {
		id_string := bits[1]
		id, err := pilosa.ParseGUID(id_string)
		if err != nil {
			return errors.New("Invalid GUID: " + id_string + " (" + key + ")")
		}
		process = self.ProcessMap.GetOrAddProcess(&id)
	}
	if len(bits) >= 3 {
		switch bits[2] {
		case "port_tcp":
			port_tcp, _ := strconv.Atoi(node.Value)
			process.SetPortTcp(port_tcp)
		case "port_http":
			port_http, _ := strconv.Atoi(node.Value)
			process.SetPortHttp(port_http)
		case "host":
			host := node.Value
			process.SetHost(host)
		}
	}

	return err
}

func (self *ProcessMapper) Run() {
	path := self.namespace + "/process"
	self_path := path + "/" + self.ID.String()

	log.Warn("Writing configuration to etcd...")
	log.Warn(self_path)

	var err error
	_, err = self.EtcdClient.Set(self_path+"/port_tcp", strconv.Itoa(self.TCPPort), 0)
	crash_on_error(err)
	_, err = self.EtcdClient.Set(self_path+"/port_http", strconv.Itoa(self.HTTPPort), 0)
	crash_on_error(err)
	_, err = self.EtcdClient.Set(self_path+"/host", self.Host, 0)
	crash_on_error(err)

	response, err := self.EtcdClient.Get(path, false, true)
	for _, node := range flatten(response.Node) {
		err := self.handlenode(node)
		if err != nil {
			out := spew.Sdump(node)
			log.Warn(err, out)
		}
	}

	receiver := make(chan *etcd.Response)
	stop := make(chan bool)
	go func() {
		// TODO: error check and restart watcher
		// TODO: use modindex to make sure watch catches everything
		_, _ = self.EtcdClient.Watch(path, 0, true, receiver, stop)
	}()

	go func() {
		for response = range receiver {
			switch response.Action {
			case "set":
				self.handlenode(response.Node)
			}
			// TODO: handle deletes
		}
	}()
}
