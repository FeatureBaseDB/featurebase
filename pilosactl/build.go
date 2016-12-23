package pilosactl

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
)

func BuildBinary(pkg, goos, goarch string) (io.Reader, error) {
	binFile, err := ioutil.TempFile("", "pilosactl")
	if err != nil {
		return nil, fmt.Errorf("build binary: %v", err)
	}
	com := exec.Command("go", "build", "-o", binFile.Name(), pkg)
	com.Env = append([]string{"GOOS=" + goos, "GOARCH=" + goarch}, os.Environ()...)

	err = com.Run()
	if err != nil {
		return nil, err
	}

	f, err := os.Open(binFile.Name())
	if err != nil {
		return nil, err
	}
	return f, nil
}
