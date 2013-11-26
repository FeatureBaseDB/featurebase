package index

import (
    "testing"
    . "github.com/smartystreets/goconvey/convey"
    "log"
    "time"
    )

    func TestBitmaps(t *testing.T) {
        log.Println("HELLO")
        Convey("function BitCount should equal method bm.Count()", t, func() {
            bm:= CreateRBBitmap()
            for i:=uint64(0); i<uint64(4096);i++{
                SetBit(bm,i)
            }
            bc1:= BitCount(bm) 
            bc2:= bm.Count()
            So(bc1, ShouldEqual, bc2)
        })
        Convey("function AND_NOT 1 and not 0 => true ", t, func() {
            bm1:= CreateRBBitmap()
            bm2:= CreateRBBitmap()
            SetBit(bm1,1)
            //SetBit(bm2,2)
            all:= AND_NOT(bm1,bm2)
            res :=BitCount(all)

            So(1, ShouldEqual, res)
        })

        Convey("UNION even + odd equal 4096 ", t, func() {
            even:= CreateRBBitmap()
            for i:=uint64(0); i<uint64(4096);i+=2{
                SetBit(even,i)
            }

            odd:= CreateRBBitmap()
            for i:=uint64(1); i<uint64(4096);i+=2{
                SetBit(odd,i)
            }
            all:= Union(even,odd)
            total_bits:= BitCount(all) 

            So(total_bits, ShouldEqual, 4096)
        })

        Convey("Intersection even - odd equal 0 ", t, func() {
            even:= CreateRBBitmap()
            for i:=uint64(0); i<uint64(4096);i+=2{
                SetBit(even,i)
            }

            odd:= CreateRBBitmap()
            for i:=uint64(1); i<uint64(4096);i+=2{
                SetBit(odd,i)
            }
            all:= Intersection(even,odd)
            total_bits:= BitCount(all) 

            So(total_bits, ShouldEqual, 0)

        })

        Convey("Bitcount< 1s ", t, func() {
            all:= CreateRBBitmap()
            for i:=uint64(0); i< uint64(65536);i++{
                SetBit(all,i)
            }
            start:= time.Now()
            BitCount(all) 
            So(start, ShouldHappenWithin, time.Duration(1) * time.Millisecond, time.Now())
    })
}
