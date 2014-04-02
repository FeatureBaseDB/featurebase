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
			ssh, err := New("50.16.204.123:22", "todd", pem_file)
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
			fmt.Println("No credentials to SSH. Test ignored.")
			So(true, ShouldEqual, true)
		}
	})
	Convey("BigFile", t, func() {
		pem_file := "id_dsa"
		if _, err := os.Stat(pem_file); err == nil {
			ssh, _ := New("50.16.204.123:22", "todd", pem_file)

			ssh.SimpleFileCopyTo("outbin", "tb")
			ssh.SimpleFileCopyFrom("tb", "outbin2")
		} else {
			fmt.Println("No credentials to SSH. Test ignored.")
			So(true, ShouldEqual, true)
		}

	})
}
