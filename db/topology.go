package db

import (
	"github.com/stathat/consistent"
	"log"
	"fmt"
	"errors"
	"strings"
	"strconv"
)

var FrameDoesNotExistError = errors.New("Frame does not exist.")

type Location struct {
	Ip string
	Port int
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
	Node string
}

// A slice is the vertical combination of every fragment. It contains the hashring used to delegate bitmaps to fragments
type Slice struct {
	Fragments []Fragment
	Hashring *consistent.Consistent
}

// A frame is a collection of slices in a given category (brands, demographics, etc), specific to a database
type Frame struct {
	Name string
	Slices []*Slice
}

// Add a slice to a frame with given Node addresses
func (f *Frame) AddSlice(addrs ...string) *Slice {
	slice := Slice{}
	slice.Hashring = consistent.New()
	slice.Hashring.NumberOfReplicas = 200
	sliceIndex := len(f.Slices)
	for index, addr := range addrs {
		slice.Fragments = append(slice.Fragments, Fragment{addr})
		slice.Hashring.Add(fmt.Sprintf("%d %d", sliceIndex, index))
	}
	f.Slices = append(f.Slices, &slice)
	return &slice
}

// Represents the entire cluster, and a reference to the Node this instance is running on
type Cluster struct {
	Databases map[string]*Database
	Self string
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

// A database is a collection of all the frames within a given profile space
type Database struct {
	Name string
	Frames []*Frame
}

// Count the number of slices in a database
func (d *Database) NumSlices() (int, error) {
	if len(d.Frames) < 1 {
		return 0, errors.New("Database is empty")
	}
	return len(d.Frames[0].Slices), nil
}

// Add a frame to a database
func (d *Database) AddFrame(name string) *Frame {
	frame := Frame{Name: name}
	d.Frames = append(d.Frames, &frame)
	return &frame
}

// Get a frame from a database
func (d *Database) GetFrame(name string) (*Frame, error) {
	for _, frame := range d.Frames {
		if frame.Name == name {
			return frame, nil
		}
	}
	return nil, FrameDoesNotExistError
}

// For debugging, prints cluster information
func (c *Cluster) Describe() {
	for _, database := range c.Databases {
		log.Println("frames", database.Frames)
		for _, frame := range database.Frames {
			log.Println(frame.Name, database.Name)
			for _, slice := range frame.Slices {
				log.Println("    ", slice)
			}
		}
	}
}

type Bitmap struct {
	FrameType string
	Id int
}
