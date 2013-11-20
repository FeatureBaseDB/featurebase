package index

import (
    "testing"
    )

func TestCount(t *testing.T) {
    bm:= CreateRBBitmap()
    for i:=uint64(0); i<uint64(4096);i++{
        SetBit(bm,i)
    }
    bc1:= BitCount(bm) 
    bc2:= bm.Count()
    if bc1 != bc2 {
        t.Errorf("Counts not equal BitCount (%d) bm.Count(%d)",bc1,bc2)
    }

    if bc1 != uint64(4096){
        t.Errorf("Count (%d) no equal 4096",bc1)
    }

}
func TestANDNOT(t *testing.T){
    bm1:= CreateRBBitmap()
    bm2:= CreateRBBitmap()
    SetBit(bm1,1)
    //SetBit(bm2,2)
    all:= AND_NOT(bm1,bm2)
    res :=BitCount(all)
    if res != 1{
        t.Errorf("SHOULD BE 1 => %d",res)
    }

    SetBit(bm2,1)
    all= AND_NOT(bm1,bm2)
    res =BitCount(all)
    if res != 0{
        t.Errorf("SHOULD BE 0 => %d",res)
    }

}
func TestUnion(t *testing.T) {
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

    if total_bits != uint64(4096){
        t.Errorf("Count (%d) no equal 4096",total_bits)
    }

}

func TestIntersect(t *testing.T){
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

    if total_bits != 0{
        t.Errorf("Count (%d) no equal 0",total_bits)
    }

}
