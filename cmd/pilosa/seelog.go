package main

import (
	"fmt"

	"github.com/umbel/pilosa/util"
)

func SeelogProductionConfig(path string, id util.GUID, level string) string {
	if path == "" {
		path = "/tmp"
	}

	fname := fmt.Sprintf("%s/pilosa.%s", path, id.String())
	//<seelog minlevel="debug" maxlevel="error">

	s := fmt.Sprintf(`<seelog minlevel="%s">
	<outputs>
		<rollingfile type="size" filename="%s" maxsize="524288000" maxrolls="4" formatid="format1" />
	</outputs> `, level, fname)

	s += `<formats>
	    <format id="format1" format="%Date/%Time [%LEV] %Msg%n"/>
	 </formats>
	</seelog>`

	return s
}
