package db

import (
	"github.com/stathat/consistent"
	"github.com/davecgh/go-spew/spew"
	"github.com/nu7hatch/gouuid"
	"log"
	"fmt"
	"errors"
	"strings"
	"strconv"
)

var FrameDoesNotExistError = errors.New("Frame does not exist.")
var SliceDoesNotExistError = errors.New("Slice does not exist.")
var FragmentDoesNotExistError = errors.New("Fragment does not exist.")
var FrameSliceIntersectDoesNotExistError = errors.New("FrameSliceIntersect does not exist.")

type Location struct {
	Ip string
	Port int
}

type Process struct {
	id *uuid.UUID
}

func NewProcess(id *uuid.UUID) *Process {
	return &Process{id}
}

// Create a Location struct given a string in form "0.0.0.0:0"
func NewLocation(location_string string) (*Location, error) {
	splitstring := strings.Split(location_string, ":")
	if len(splitstring) != 2 {
		return nil, errors.New("Location string must be in form 0.0.0.0:0")
	}
	ip := splitstring[0]
	port, err := strconv.Atoi(splitstring[1])
	if err != nil{
		return nil, errors.New("Port is not a number!")
	}
	return &Location{ip, port}, nil
}

func (location *Location) ToString() string {
	return fmt.Sprintf("%s:%d", location.Ip, location.Port)
}

// Map of node location to their router
type NodeMap map[Location]Location

// A fragment is a collection of bitmaps within a slice. The fragment contains a reference to the responsible node for that fragment. The node is in the form ip:port
type Fragment struct {
    process *Process
	id int
}

// A slice is the vertical combination of every fragment. It contains the hashring used to delegate bitmaps to fragments
type Slice struct {
    id int
}

// A frame is a collection of slices in a given category (brands, demographics, etc), specific to a database
type Frame struct {
	Name string
}

type FrameSliceIntersect struct {
    slice *Slice
    frame *Frame
	Fragments []*Fragment
	Hashring *consistent.Consistent
}

func (fsi *FrameSliceIntersect) GetFragment(fragment_id int) (*Fragment, error) {
	for _, fragment := range fsi.Fragments {
		if fragment.id == fragment_id {
			return fragment, nil
		}
	}
	return nil, FragmentDoesNotExistError
}

// Add a slice to a database
func (d *Database) AddSlice(slice_id int) *Slice {
	slice := Slice{id: slice_id}
	d.slices = append(d.slices, &slice)
    // add intersections
	for _, frame := range d.frames {
        d.AddFrameSliceIntersect(frame, &slice)
    }
	return &slice
}

// Represents the entire cluster, and a reference to the Node this instance is running on
type Cluster struct {
	Databases map[string]*Database
}

func NewCluster() *Cluster {
	cluster := Cluster{}
	cluster.Databases = make(map[string]*Database)
	return &cluster
}

// Add a database to a cluster
func (c *Cluster) AddDatabase(name string) *Database {
	database := Database{Name: name}
	if c.Databases == nil {
		c.Databases = make(map[string]*Database)
	}
	c.Databases[name] = &database
	return &database
}

func (c *Cluster) GetDatabase(name string) (*Database, error) {
	value, ok := c.Databases[name]
	if !ok {
		return nil, errors.New("The database does not exist!!!!!!!!!!!!!!!!!!!!!!!! -HO")
	} else {
		return value, nil
	}
}

// A database is a collection of all the frames within a given profile space
type Database struct {
	Name string
	frames []*Frame
	slices []*Slice
    FrameSliceIntersects []*FrameSliceIntersect
}

// Count the number of slices in a database
func (d *Database) NumSlices() (int, error) {
	if len(d.slices) < 1 {
		return 0, errors.New("Database is empty")
	}
	return len(d.slices), nil
}

// Add a frame to a database
func (d *Database) AddFrame(name string) *Frame {
	frame := Frame{Name: name}
	d.frames = append(d.frames, &frame)
    // add intersections
	for _, slice := range d.slices {
        d.AddFrameSliceIntersect(&frame, slice)
    }
	return &frame
}

func (d *Database) AddFragment(frame *Frame, slice *Slice, process *Process, fragment_id int) *Fragment {

	frameslice, _ := d.GetFrameSliceIntersect(frame, slice)
    fragment := Fragment{process: process, id: fragment_id}
    frameslice.Fragments = append(frameslice.Fragments, &fragment)

    frameslice.Hashring.Add(fmt.Sprintf("%d", fragment_id))

    return &fragment
}

func (d *Database) AddFrameSliceIntersect(frame *Frame, slice *Slice) *FrameSliceIntersect {
	frameslice := FrameSliceIntersect{frame: frame, slice: slice}
	d.FrameSliceIntersects = append(d.FrameSliceIntersects, &frameslice)

	frameslice.Hashring = consistent.New()
	frameslice.Hashring.NumberOfReplicas = 16

	return &frameslice
}

func (d *Database) GetFrameSliceIntersect(frame *Frame, slice *Slice) (*FrameSliceIntersect, error) {
	for _, frameslice := range d.FrameSliceIntersects {
		if frameslice.frame == frame && frameslice.slice == slice {
			return frameslice, nil
		}
	}
	return nil, FrameSliceIntersectDoesNotExistError
}

// Get a frame from a database
func (d *Database) GetFrame(name string) (*Frame, error) {
	for _, frame := range d.frames {
		if frame.Name == name {
			return frame, nil
		}
	}
	return nil, FrameDoesNotExistError
}

// Get a slice from a database
func (d *Database) GetSlice(id int) (*Slice, error) {
	for _, slice := range d.slices {
		if slice.id == id {
			return slice, nil
		}
	}
	return nil, SliceDoesNotExistError
}

// Get a slice from a database
func (d *Database) GetSliceForProfile(profile_id int) (*Slice, error) {
    log.Println("GetSliceForProfile")
    log.Println("profile_id:",profile_id)
    slice_id := profile_id / SLICE_WIDTH
    return d.GetSlice(slice_id)
}


type Bitmap struct {
	Id int
	FrameType string
}

func (d *Database) TestSetBit(bitmap Bitmap, profile_id int) {
    log.Println("TestSetBit")
    slice, _ := d.GetSliceForProfile(profile_id)
    log.Println("slice:",slice)
    frame, _ := d.GetFrame(bitmap.FrameType)
    fsi, _ := d.GetFrameSliceIntersect(frame, slice)
    frag_id_s,err := fsi.Hashring.Get(fmt.Sprintf("%d", bitmap.Id))
	frag_id, err := strconv.Atoi(frag_id_s)
	fragment, err := fsi.GetFragment(frag_id)
	if err != nil {
		log.Fatal(err)
	}
	spew.Dump(fragment)
}
