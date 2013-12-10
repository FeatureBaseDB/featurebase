package db

import (
    "testing"
    "log"
    "github.com/nu7hatch/gouuid"
    "github.com/davecgh/go-spew/spew"
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
        database := cluster.GetOrCreateDatabase("main")

        frame := database.GetOrCreateFrame("general")
        slice := database.GetOrCreateSlice(0)

        fragment_id, _ := uuid.ParseHex("6a9aea17-2915-4eb4-858f-a8d7d4dc0a1e")
        spew.Dump(fragment_id)
        database.GetOrCreateFragment(frame, slice, fragment_id)


        spew.Dump(database)
        /*
        log.Println(database)
        log.Println("----------------------------------")
        for _, fsi := range database.frame_slice_intersects {
            log.Println(fsi.frame,fsi.slice)
        }
        num_slices, _ := database.NumSlices()
        log.Println(num_slices)
        */

        //frame, _ := database.GetFrame("general")
        //slice, _ := database.GetSlice(0)

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

        //uuid, _ := uuid.ParseHex("6a9aea17-2915-4eb4-858f-a8d7d4dc0a1e")
        //spew.Dump(uuid)
        ////database.AddFragment(frame, slice, uuid)
        /*
        database.AddFragment(frame, slice, process)
        database.AddFragment(frame, slice, process)
        database.AddFragment(frame, slice, process)
        database.AddFragment(frame, slice, process)
        database.AddFragment(frame, slice, process)
        */


        //fsi, _ := database.GetFrameSliceIntersect(frame, slice)
        //log.Println(fsi)
        //log.Println(fsi.Hashring)

        /*
        slicer, errer := database.GetSliceForProfile(131072)
        log.Println(slicer)
        log.Println(errer)
        database.AddSlice()
        slicer, errer = database.GetSliceForProfile(131072)
        log.Println(slicer)
        log.Println(errer)
        */

        ////bitmap := Bitmap{Id: 555, FrameType: "general"}
        ////log.Println("bitmap:",bitmap)

        /*
        profile_id := 65535
        fragment, _ := database.OldGetFragment(bitmap, profile_id)

        spew.Dump("FRAGMENT")
        spew.Dump(fragment)
        */
        spew.Dump("DONE")

    })
}
