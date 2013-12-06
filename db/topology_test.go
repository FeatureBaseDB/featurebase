package db

import (
    "testing"
    "log"
    . "github.com/smartystreets/goconvey/convey"
)

func TestTopology(t *testing.T) {
    Convey("Basic DB structures", t, func() {
        log.Println("topology test")

        /*
        cluster := Cluster{Self:"192.168.1.100:1201"}
        database := cluster.AddDatabase("property49")
        frame := database.AddFrame("general")
        frame.AddSlice("192.168.1.100:1201", "192.168.1.100:1202", "192.168.1.100:1203")

        log.Println(cluster)
        log.Println(database)
        log.Println(frame)
        */

        cluster := NewCluster()
        database := cluster.AddDatabase("property49")
        database.AddFrame("general")
        //database.AddFrame("brands")
        database.AddSlice(0)
        database.AddSlice(1)
        //database.AddSlice()

        log.Println(database)
        log.Println("----------------------------------")
        for _, fsi := range database.FrameSliceIntersects {
            log.Println(fsi.frame,fsi.slice)
        }
        num_slices, _ := database.NumSlices()
        log.Println(num_slices)


        frame, _ := database.GetFrame("general")
        slice, _ := database.GetSlice(0)

        //loc1, _ := NewLocation("192.168.1.100:8001")
        /*
        loc2, _ := NewLocation("192.168.1.100:8002")
        loc3, _ := NewLocation("192.168.1.100:8003")
        loc4, _ := NewLocation("192.168.1.100:8004")
        loc5, _ := NewLocation("192.168.1.100:8005")
        */

        /*
        database.AddFragment(frame, slice, loc1, 0)
        database.AddFragment(frame, slice, loc1, 1)
        database.AddFragment(frame, slice, loc1, 2)
        database.AddFragment(frame, slice, loc1, 3)
        database.AddFragment(frame, slice, loc1, 4)
        */


        fsi, _ := database.GetFrameSliceIntersect(frame, slice)
        log.Println(fsi)
        //log.Println(fsi.Hashring)

        x,_ := fsi.Hashring.Get("able")
        log.Println(x)

        /*
        slicer, errer := database.GetSliceForProfile(131072)
        log.Println(slicer)
        log.Println(errer)
        database.AddSlice()
        slicer, errer = database.GetSliceForProfile(131072)
        log.Println(slicer)
        log.Println(errer)
        */


        bitmap := Bitmap{Id: 555, FrameType: "general"}
        log.Println("bitmap:",bitmap)

        database.TestSetBit(bitmap, 65535)


    })
}
