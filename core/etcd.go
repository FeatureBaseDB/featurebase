package core

import (
	"errors"
	"fmt"
	"log"
	"pilosa/config"
	"pilosa/db"
	"pilosa/util"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/coreos/go-etcd/etcd"
	"github.com/davecgh/go-spew/spew"
)

type TopologyMapper struct {
	service   *Service
	namespace string
}

func (self *TopologyMapper) Setup() {
	log.Println(self.namespace + "/db")
	db_path := self.namespace + "/db"
	resp, err := self.service.Etcd.Get(db_path, false, true)
	if err != nil {
		ee, ok := err.(*etcd.EtcdError)
		if ok && ee.ErrorCode == 100 { // node does not exist
			resp, err = self.service.Etcd.CreateDir(db_path, 0)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}
	}
	//need to lock the world
	for _, node := range flatten(resp.Node) {
		err := self.handlenode(node)
		if err != nil {
			log.Println(err)
		}
	}

}
func (self *TopologyMapper) Run() {

	receiver := make(chan *etcd.Response)
	stop := make(chan bool)
	go func() {
		// TODO: add some terminating measure
		// TODO: use modindex to make sure watch catches everything
		for {
			ns := self.namespace + "/db"
			log.Println(" ETCD watcher:", ns)
			resp, err := self.service.Etcd.Watch(ns, 0, true, receiver, stop)
			log.Println("TopologyMapper ETCD watcher", resp, err)
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

func NewTopologyMapper(service *Service, namespace string) *TopologyMapper {
	return &TopologyMapper{service, namespace}
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
	}
	sort.Sort(processlist)

	return processlist[0], nil
}

func (self *TopologyMapper) MakeFragments(db string, slice_int int) error {
	/*
		frames_to_create := config.GetStringArrayDefault("supported_frames", []string{"b.n", "l.n", "t.t", "d"})
		for _, frame := range frames_to_create {
			err := self.AllocateFragment(db, frame, slice_int)
			if err != nil {
				log.Println(err)
			}
		}
	*/
	return nil

}

func (self *TopologyMapper) AllocateFragment(db, frame string, slice_int int) error {
	//get Lock to create the fragment
	ttl := uint64(config.GetIntDefault("fragment_alloc_lock_time_secs", 600))
	lock_key := fmt.Sprintf("%s/lock/%s-%s-%d", self.namespace, db, frame, slice_int)
	response, err := self.service.Etcd.RawCreate(lock_key, "0", ttl)

	if err == nil {
		if response.StatusCode == 201 { //key created
			//figure out least loaded process..possibly check max process
			//to create the node, just write off the items to etcd and the watch should spawn
			//be nice if something would notify perhaps queue
			m := make(map[string]int)
			id_string := self.service.Id.String()
			m[id_string] = 0 //at least have one process if none created
			for _, dbs := range self.service.Cluster.GetDatabases() {
				for _, fsi := range dbs.GetFramSliceIntersects() {
					for _, fragment := range fsi.GetFragments() {
						process := fragment.GetProcess().Id().String()
						i := m[process]
						i++
						m[process] = i

					}

				}

			}
			p, err := getLightestProcess(m)
			if err != nil {
				return err
			}

			// PUT -d "value=5cb315c3-6e1d-4218-89b7-943d1dba985b" http://etcd0:4001/v2/keys/pilosa/0/db/29/frame/d/slice/5/fragment/a2b632fc4001b817/proces
			//so i need db, frame, slice , fragment_id
			fuid := util.SUUID_to_Hex(util.Id())
			fragment_key := fmt.Sprintf("%s/db/%s/frame/%s/slice/%d/fragment/%s/process", self.namespace, db, frame, slice_int, fuid)
			process_guid := p.Key
			// need to check value to see how many we have left
			_, err = self.service.Etcd.Set(fragment_key, process_guid, 0)
			log.Printf("Fragment sent to etcd: %s(%s)", fragment_key, process_guid)
		}

	}
	return err

}

func (self *TopologyMapper) handlenode(node *etcd.Node) error {
	key := node.Key[len(self.namespace)+1:]
	bits := strings.Split(key, "/")
	var database *db.Database
	var frame *db.Frame
	var fragment *db.Fragment
	var fragment_id util.SUUID
	var slice *db.Slice
	var slice_int int
	var process_uuid util.GUID
	var process *db.Process
	var err error

	if len(bits) <= 1 || bits[0] != "db" {
		return nil
	}
	if len(bits) > 1 {
		database = self.service.Cluster.GetOrCreateDatabase(bits[1])
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
		fragment_id = util.Hex_to_SUUID(bits[7])
		fragment = database.GetOrCreateFragment(frame, slice, fragment_id)
	}

	if len(bits) > 8 {
		if bits[8] != "process" {
			return errors.New("no process")
		}
		process_uuid, err = util.ParseGUID(node.Value)
		if err != nil {
			return err
		}
		process = db.NewProcess(&process_uuid)
		fragment.SetProcess(process)

		if self.service.Id.String() == process_uuid.String() {
			self.service.Index.AddFragment(bits[1], bits[3], slice_int, fragment_id)
		}

	}
	return err
}
func (self *TopologyMapper) remove_fragment(node *etcd.Node) error {
	log.Println(" hot remove_fragment (Not Supported yet):", node)
	/*
		key := node.Key[len(self.namespace)+1:]
		bits := strings.Split(key, "/")
		process_uuid, err = util.ParseGUID(node.Value)
		if self.service.Id.String() == process_uuid.String() {
			fragment_id = util.Hex_to_SUUID(bits[7])
			self.service.Index.RemoveFragment(fragment_id)
		}
	*/
	return nil
}

func flatten(node *etcd.Node) []*etcd.Node {
	nodes := []*etcd.Node{node}
	for i := 0; i < len(node.Nodes); i++ {
		nodes = append(nodes, flatten(node.Nodes[i])...)
	}
	return nodes
}

type Node struct {
	id        *util.GUID
	ip        string
	port_tcp  int
	port_http int
}

type ProcessMap struct {
	nodes map[util.GUID]*db.Process
	mutex sync.Mutex
}

func NewProcessMap() *ProcessMap {
	p := ProcessMap{}
	p.nodes = make(map[util.GUID]*db.Process)
	return &p
}

func (self *ProcessMap) AddProcess(process *db.Process) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.nodes[process.Id()] = process
}

func (self *ProcessMap) GetProcess(id *util.GUID) (*db.Process, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	process, ok := self.nodes[*id]
	if !ok {
		return nil, errors.New("No such process")
	}
	return process, nil
}

func (self *ProcessMap) GetOrAddProcess(id *util.GUID) *db.Process {
	process, err := self.GetProcess(id)
	if err != nil {
		process = db.NewProcess(id)
		self.AddProcess(process)
	}
	return process
}

func (self *ProcessMap) GetHost(id *util.GUID) (string, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	process, ok := self.nodes[*id]
	if !ok {
		return "", errors.New("Process does not exist")
	}
	return process.Host(), nil
}

func (self *ProcessMap) GetPortTcp(id *util.GUID) (int, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	process, ok := self.nodes[*id]
	if !ok {
		return 0, errors.New("Process does not exist")
	}
	return process.PortTcp(), nil
}

func (self *ProcessMap) GetPortHttp(id *util.GUID) (int, error) {
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
	service   *Service
	receiver  chan etcd.Response
	commands  chan ProcessMapperCommand
	namespace string
}

func NewProcessMapper(service *Service, namespace string) *ProcessMapper {
	return &ProcessMapper{
		service:   service,
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

func (self *ProcessMapper) getnode(u *util.GUID) *Node {
	return new(Node)
}

func crash_on_error(err error) {
	if err != nil {
		log.Fatal(err)
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
		id, err := util.ParseGUID(id_string)
		if err != nil {
			return errors.New("Invalid GUID: " + id_string)
		}
		process = self.service.ProcessMap.GetOrAddProcess(&id)
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
	id_string := self.service.Id.String()
	path := self.namespace + "/process"
	self_path := path + "/" + id_string

	log.Println("Writing configuration to etcd...")
	log.Println(self_path)

	var err error
	_, err = self.service.Etcd.Set(self_path+"/port_tcp", strconv.Itoa(config.GetInt("port_tcp")), 0)
	crash_on_error(err)
	_, err = self.service.Etcd.Set(self_path+"/port_http", strconv.Itoa(config.GetInt("port_http")), 0)
	crash_on_error(err)
	_, err = self.service.Etcd.Set(self_path+"/host", config.GetString("host"), 0)
	crash_on_error(err)

	response, err := self.service.Etcd.Get(path, false, true)
	for _, node := range flatten(response.Node) {
		err := self.handlenode(node)
		if err != nil {
			spew.Dump(node)
			log.Println(err)
		}
	}

	receiver := make(chan *etcd.Response)
	stop := make(chan bool)
	go func() {
		// TODO: error check and restart watcher
		// TODO: use modindex to make sure watch catches everything
		_, _ = self.service.Etcd.Watch(path, 0, true, receiver, stop)
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
