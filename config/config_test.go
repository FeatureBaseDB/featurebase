package config

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestConfig(t *testing.T) {
	err := os.Setenv("PILOSA_CONFIG", "test.yaml")
	if err != nil {
		t.Fatal("Error setting PILOSA_CONFIG")
	}
	Convey("config.Get()", t, func() {
		So(Get("port_tcp"), ShouldEqual, 12000)
		So(Get("port_http"), ShouldEqual, 15000)
		So(Get("temp"), ShouldEqual, "/tmp")
		So(Get("notfound"), ShouldBeNil)
	})
	Convey("config.GetSafe()", t, func() {
		val, ok := GetSafe("port_tcp")
		So(ok, ShouldBeTrue)
		So(val, ShouldEqual, 12000)
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

func TestConfigObject(t *testing.T) {
	err := os.Setenv("PILOSA_CONFIG", "")
	if err != nil {
		t.Fatal("Error setting PILOSA_CONFIG")
	}
	conf := NewConfig("test.yaml")
	Convey("config.Get()", t, func() {
		So(conf.Get("port_tcp"), ShouldEqual, 12000)
		So(conf.Get("port_http"), ShouldEqual, 15000)
		So(conf.Get("temp"), ShouldEqual, "/tmp")
		So(conf.Get("notfound"), ShouldBeNil)
	})
	Convey("config.GetSafe()", t, func() {
		val, ok := conf.GetSafe("port_tcp")
		So(ok, ShouldBeTrue)
		So(val, ShouldEqual, 12000)
		val, ok = conf.GetSafe("derp")
		So(ok, ShouldBeFalse)
		So(val, ShouldBeNil)
	})
	Convey("config.GetInt()", t, func() {
		So(conf.GetInt("port_tcp"), ShouldEqual, 12000)
		So(conf.GetInt("port_http"), ShouldEqual, 15000)
		So(conf.GetInt("notfound"), ShouldEqual, 0)
	})
	Convey("config.GetString()", t, func() {
		So(conf.GetString("temp"), ShouldEqual, "/tmp")
	})
}
