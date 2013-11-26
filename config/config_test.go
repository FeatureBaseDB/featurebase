package config

import (
	"testing"
	"os"
	. "github.com/smartystreets/goconvey/convey"
)

func TestConfig(t *testing.T) {
	err := os.Setenv("PILOSA_CONFIG", "test.yaml")
	if err != nil {
		t.Fatal("Error setting PILOSA_CONFIG")
	}
	Convey("config.Get()", t, func() {
		So(Get("port_tcp").(int), ShouldEqual, 12000)
		So(Get("port_http").(int), ShouldEqual, 15000)
		So(Get("temp").(string), ShouldEqual, "/tmp")
		So(Get("notfound"), ShouldBeNil)
	})
	Convey("config.GetSafe()", t, func() {
		val, ok := GetSafe("port_tcp")
		So(ok, ShouldBeTrue)
		So(val.(int), ShouldEqual, 12000)
		val, ok = GetSafe("derp")
		So(ok, ShouldBeFalse)
		So(val, ShouldBeNil)
	})
	Convey("config.GetInt()", t, func() {
		So(GetInt("port_tcp"), ShouldEqual, 12000)
		So(GetInt("port_http"), ShouldEqual, 15000)
		So(GetInt("notfound"), ShouldEqual, 0)
	})
	Convey("config.GetString()", t, func() {
		So(GetString("temp"), ShouldEqual, "/tmp")
	})
}
