package query

//package main
//
//import (
//	"pilosa/query"
//	"pilosa/core"
//	"math/rand"
//	"github.com/nu7hatch/gouuid"
//	"time"
//	"log"
//)
//
//func main() {
//	rand.Seed(time.Now().UnixNano())
//	cluster := core.Cluster{Self:"192.168.1.100:1201"}
//	database := cluster.AddDatabase("property49")
//	frame := database.AddFrame("general")
//	frame.AddSlice("192.168.1.100:1201", "192.168.1.100:1202", "192.168.1.100:1203")
//	frame.AddSlice("192.168.1.101:1201", "192.168.1.101:1202", "192.168.1.101:1203")
//	frame.AddSlice("192.168.1.102:1201", "192.168.1.102:1202", "192.168.1.102:1203")
//	frame2 := database.AddFrame("brands")
//	frame2.AddSlice("192.168.1.200:1201", "192.168.1.200:1202", "192.168.1.200:1203")
//	frame2.AddSlice("192.168.1.201:1201", "192.168.1.201:1202", "192.168.1.201:1203")
//	frame2.AddSlice("192.168.1.202:1201", "192.168.1.202:1202", "192.168.1.202:1203")
//
//	//cluster.describe()
//
//	//query := Query{"get", []QueryInput{Bitmap{"general", 10}}}
//
//	//query := Query{"union", []QueryInput{
//	//	&Query{"get", []QueryInput{Bitmap{"general", 20}}},
//	//	&Query{"get", []QueryInput{Bitmap{"brands", 30}}},
//	//}}
//
//	//queryString = "union(bitmap(general, 33), bitmap(brands, 44))"
//	//queryString := `["union", ["intersect", ["bitmap", "general", 33], ["bitmap", "brands", 44]], ["bitmap", "general", 55]]`
//	//query := `count(union(bitmap(""), bitmap(brands, 55)))`
//	//query := `setbit(bitmap(cats, 33), 75000)`
//	queryString := `["union", ["intersect", ["bitmap", "general", 33], ["bitmap", "brands", 44]], ["bitmap", "general", 55]]`
//	queryParser := query.QueryParser{queryString}
//	queryParsed, err := queryParser.Parse()
//	log.Println(queryParsed)
//	if err != nil {
//		log.Println("ERROR!", err)
//	}
//
////	query := qp.Query{"inter", []qp.QueryInput{
////		&qp.Query{"union", []qp.QueryInput{
////			&qp.Query{"get", []qp.QueryInput{core.Bitmap{"general", 20}}},
////			&qp.Query{"get", []qp.QueryInput{core.Bitmap{"brands", 30}}},
////		}},
////		&qp.Query{"get", []qp.QueryInput{core.Bitmap{"general", 10}}},
////	}}
////
//	planner := query.QueryPlanner{&cluster, database}
//
//	id, _ := uuid.NewV4()
//	dest := "1.2.3.4:1234"
//	log.Println("Query id", id, "Dest", dest)
//
//	queryplan := planner.Plan(queryParsed, id, dest, -1)
//
//	for _, i := range *queryplan {
//		log.Println(i)
//	}
//	//fmt.Println(queryplan)
//}
