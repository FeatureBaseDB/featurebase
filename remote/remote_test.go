package remote

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	. "github.com/smartystreets/goconvey/convey"
)

func TestRemote(t *testing.T) {

	Convey("Login", t, func() {
		pem_file := "id_dsa"
		if _, err := os.Stat(pem_file); err == nil {
			ssh, err := New("50.16.204.123:22", "todd", "id_dsa")
			So(err, ShouldEqual, nil)
			err = ssh.Launch("sleep 10 && date", "background")
			So(err, ShouldEqual, nil)
			content, _ := ssh.Run("date", false)
			ssh.CopyTo(strings.NewReader(content), "uploadfile")
			var out bytes.Buffer
			ssh.CopyFrom("uploadfile", &out)

			ok := bytes.Equal([]byte(content), out.Bytes())
			So(ok, ShouldEqual, true)
		} else {
			fmt.Println("No credentials so SSH Test ignored")
			So(true, ShouldEqual, true)
		}
	})
	/*
		Convey("BigFile", t, func() {
			ssh, err := New("50.16.204.123:22", "todd", "id_dsa")

			fo, err := os.Create("outbin")
			if err != nil {
				panic(err)
			}
			// close fo on exit and check for its returned error
			defer func() {
				if err := fo.Close(); err != nil {
					panic(err)
				}
			}()
			// make a write buffer
			w := bufio.NewWriter(fo)
			ssh.CopyFrom("pilosa-cruncher", w)
			if err = w.Flush(); err != nil {
				panic(err)
			}

		})
	*/
}
