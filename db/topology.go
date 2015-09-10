package db

import (
	"errors"
	"fmt"
	"sync"

	log "github.com/cihub/seelog"
	"github.com/stathat/consistent"
	"github.com/umbel/pilosa"
)

// SupportedFrames is a list of frame types that are supported.
var SupportedFrames = []string{"default"}

var FrameDoesNotExistError = errors.New("Frame does not exist.")
var InvalidFrameError = errors.New("Invalid frame.")
var SliceDoesNotExistError = errors.New("Slice does not exist.")
var FragmentDoesNotExistError = errors.New("Fragment does not exist.")
var FrameSliceIntersectDoesNotExistError = errors.New("FrameSliceIntersect does not exist.")

type Location struct {
	ProcessId  *pilosa.GUID
	FragmentId pilosa.SUUID
}

type Process struct {
	id        *pilosa.GUID
	host      string
	port_tcp  int
	port_http int
	mutex     sync.Mutex
}

func NewProcess(id *pilosa.GUID) *Process {
	return &Process{id: id}
}

func (self *Process) Id() pilosa.GUID {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return *self.id
}

func (self *Process) Host() string {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.host
}

func (self *Process) SetHost(host string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.host = host
}

func (self *Process) PortTcp() int {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.port_tcp
}

func (self *Process) SetPortTcp(port int) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.port_tcp = port
}

func (self *Process) PortHttp() int {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.port_http
}

func (self *Process) SetPortHttp(port int) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.port_http = port
}

/////////// CLUSTERS
//////////////////////////////////////////////////////////////////////

// Represents the entire cluster, and a reference to the Node this instance is
// running on
type Cluster struct {
	databases map[string]*Database
	mutex     sync.Mutex
}

func NewCluster() *Cluster {
	cluster := Cluster{}
	cluster.databases = make(map[string]*Database)
	return &cluster
}
func (self *Cluster) GetDatabases() map[string]*Database {
	return self.databases

}

func (self *Cluster) IsValidDatabase(dbname string) bool {
	for name, _ := range self.GetDatabases() {
		if name == dbname {
			return true
		}

	}
	return false
}

/////////// DATABASES
//////////////////////////////////////////////////////////////////////

// A database is a collection of all the frames within a given profile space
type Database struct {
	Name                   string
	frames                 []*Frame
	slices                 []*Slice
	frame_slice_intersects []*FrameSliceIntersect
	mutex                  sync.Mutex
}

func (self *Database) GetFrameSliceIntersects() []*FrameSliceIntersect {
	return self.frame_slice_intersects
}

// Add a database to a cluster
func (c *Cluster) addDatabase(name string) *Database {
	database := Database{Name: name}
	if c.databases == nil {
		c.databases = make(map[string]*Database)
	}
	c.databases[name] = &database
	return &database
}

func (c *Cluster) getDatabase(name string) (*Database, error) {
	value, ok := c.databases[name]
	if !ok {
		return nil, errors.New("The database does not exist!")
	} else {
		return value, nil
	}
}

func (c *Cluster) GetOrCreateDatabase(name string) *Database {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	database, err := c.getDatabase(name)
	if err == nil {
		return database
	}
	return c.addDatabase(name)
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func (d *Database) IsValidFrame(name string) bool {
	return stringInSlice(name, SupportedFrames)
}

// Count the number of slices in a database
func (d *Database) NumSlices() (int, error) {
	if len(d.slices) < 1 {
		return 0, errors.New("Database is empty")
	}
	return len(d.slices), nil
}

// return the slice_ids that are in a database
func (d *Database) SliceIds() ([]int, error) {
	var rtn []int
	for _, slice := range d.slices {
		rtn = append(rtn, slice.id)
	}
	return rtn, nil
}

///////// FRAMES
//////////////////////////////////////////////////////////////////////

// A frame is a collection of slices in a given category
// (brands, demographics, etc), specific to a database
type Frame struct {
	name string
}

// Get a frame from a database
func (d *Database) getFrame(name string) (*Frame, error) {
	// should we check here for supported frames?
	if !d.IsValidFrame(name) {
		return nil, InvalidFrameError
	}
	for _, frame := range d.frames {
		if frame.name == name {
			return frame, nil
		}
	}
	return nil, FrameDoesNotExistError
}

// Add a frame to a database
func (d *Database) addFrame(name string) *Frame {
	frame := Frame{name: name}
	d.frames = append(d.frames, &frame)
	// add intersections
	for _, slice := range d.slices {
		d.AddFrameSliceIntersect(&frame, slice)
	}
	return &frame
}

func (d *Database) GetOrCreateFrame(name string) *Frame {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	frame, err := d.getFrame(name)
	if err == nil {
		return frame
	}
	return d.addFrame(name)
}

///////// SLICES
///////////////////////////////////////////////////////////////////////////

// A slice is the vertical combination of every fragment.
type Slice struct {
	id int
}

func (self *Slice) Id() int {
	return self.id
}

// Get a slice from a database
func (d *Database) getSlice(slice_id int) (*Slice, error) {
	for _, slice := range d.slices {
		if slice.id == slice_id {
			return slice, nil
		}
	}
	return nil, SliceDoesNotExistError
}

// Add a slice to a database
func (d *Database) addSlice(slice_id int) *Slice {
	slice := Slice{id: slice_id}
	d.slices = append(d.slices, &slice)
	// add intersections
	for _, frame := range d.frames {
		d.AddFrameSliceIntersect(frame, &slice)
	}
	return &slice
}

func (d *Database) GetOrCreateSlice(slice_id int) *Slice {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	slice, err := d.getSlice(slice_id)
	if err == nil {
		return slice
	}
	return d.addSlice(slice_id)
}

///////// FRAME-SLICE INTERSECT
////////////////////////////////////////////////////////////////

type FrameSliceIntersect struct {
	frame     *Frame
	slice     *Slice
	fragments []*Fragment
	hashring  *consistent.Consistent
}

func (d *Database) AddFrameSliceIntersect(frame *Frame, slice *Slice) *FrameSliceIntersect {
	frameslice := FrameSliceIntersect{frame: frame, slice: slice}
	d.frame_slice_intersects = append(d.frame_slice_intersects, &frameslice)
	frameslice.hashring = consistent.New()
	frameslice.hashring.NumberOfReplicas = 16
	return &frameslice
}

func (d *Database) GetFrameSliceIntersect(frame *Frame, slice *Slice) (*FrameSliceIntersect, error) {
	for _, frameslice := range d.frame_slice_intersects {
		if frameslice.frame == frame && frameslice.slice == slice {
			return frameslice, nil
		}
	}
	log.Warn("Missing FrameSliceIntersect:", d.Name, frame, slice)
	return nil, FrameSliceIntersectDoesNotExistError
}

func (self *FrameSliceIntersect) GetFragments() []*Fragment {
	return self.fragments
}

func (d *Database) GetFragment(fragment_id pilosa.SUUID) (*Fragment, error) {
	for _, fsi := range d.frame_slice_intersects {
		f, err := fsi.GetFragment(fragment_id)
		if err == nil {
			return f, nil
		}
	}
	return nil, FragmentDoesNotExistError
}
func (self *FrameSliceIntersect) GetFragment(fragment_id pilosa.SUUID) (*Fragment,
	error) {
	for _, fragment := range self.fragments {
		if fragment.id == fragment_id {
			return fragment, nil
		}
	}
	return nil, FragmentDoesNotExistError
}

func (self *FrameSliceIntersect) AddFragment(fragment *Fragment) {
	self.fragments = append(self.fragments, fragment)
	self.hashring.Add(fragment.id.String())
}

///////// FRAGMENTS
//////////////////////////////////////////////////////////////////////////

// A fragment is a collection of bitmaps within a slice. The fragment contains a
// reference to the responsible node for that fragment. The node is in the form
// ip:port
type Fragment struct {
	id      pilosa.SUUID
	process *Process
}

func (self *Fragment) GetId() pilosa.SUUID {
	return self.id
}

func (self *Fragment) GetProcess() *Process {
	return self.process
}

func (self *Fragment) GetProcessId() *pilosa.GUID {
	return self.process.id
}

func (self *Fragment) GetLocation() *Location {
	return &Location{self.process.id, self.id}
}

// rename this one
func (d *Database) GetFragmentForBitmap(slice *Slice, bitmap *Bitmap) (*Fragment, error) {
	frame, err := d.getFrame(bitmap.FrameType)
	if err != nil {
		log.Warn("Missing FrameType", bitmap.FrameType, d.Name, slice)
		log.Warn(err)
		return nil, err
	}
	fsi, err := d.GetFrameSliceIntersect(frame, slice)
	if err != nil {
		log.Warn("Missing frame,slice", frame, slice)
		log.Warn(err)
		return nil, err
	}
	frag_id_s, err := fsi.hashring.Get(fmt.Sprintf("%d", bitmap.Id))
	if err != nil {
		log.Warn("ERROR FSI.GET:", bitmap.Id, bitmap.FrameType, d.Name, frame, slice)
		log.Warn(err)
		return nil, err
	}
	frag_id := pilosa.ParseSUUID(frag_id_s)
	return fsi.GetFragment(frag_id)
}

func (d *Database) GetFragmentForFrameSlice(frame *Frame, slice *Slice) (*Fragment, error) {
	fsi, err := d.GetFrameSliceIntersect(frame, slice)
	if err != nil {
		log.Warn("Missing frame,slice", frame, slice)
		log.Warn(err)
		return nil, err
	}
	frag_id_s, err := fsi.hashring.Get("0")
	// we don't need a specific bitmap in here because we're assuming the hashring only has a single element
	if err != nil {
		log.Warn("ERROR FSI.GET:", d.Name, frame, slice)
		log.Warn(err)
		return nil, err
	}
	frag_id := pilosa.ParseSUUID(frag_id_s)
	return fsi.GetFragment(frag_id)
}

func (d *Database) getFragment(frame *Frame, slice *Slice, fragment_id pilosa.SUUID) (*Fragment, error) {
	fsi, err := d.GetFrameSliceIntersect(frame, slice)
	if err != nil {
		log.Warn(err)
		return nil, err
	}
	return fsi.GetFragment(fragment_id)
}

func (d *Database) addFragment(frame *Frame, slice *Slice, fragment_id pilosa.SUUID) *Fragment {
	fsi, err := d.GetFrameSliceIntersect(frame, slice)
	if err != nil {
		log.Warn("database.addFragment", err)
		return nil
	}
	fragment := Fragment{id: fragment_id}
	fsi.AddFragment(&fragment)
	return &fragment
}

func (d *Database) GetOrCreateFragment(frame *Frame, slice *Slice, fragment_id pilosa.SUUID) *Fragment {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	fragment, err := d.getFragment(frame, slice, fragment_id)
	if err == nil {
		return fragment
	}
	return d.addFragment(frame, slice, fragment_id)
}

func (f *Fragment) SetProcess(process *Process) {
	f.process = process
}
func GetSlice(profile_id uint64) int {
	return int(profile_id / SLICE_WIDTH)
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Get a slice from a database

func (d *Database) GetSliceForProfile(profile_id uint64) (*Slice, error) {
	return d.getSlice(GetSlice(profile_id))
}

type Bitmap struct {
	Id        uint64
	FrameType string
	Filter    uint64
}
