package main

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestFileExists(t *testing.T) {
	fileName := "missing"
	if x, _ := fileExists(fileName); x {
		t.Fatalf("file %v doesn't exist", fileName)
	}
	file, err := os.Create(fileName)
	if err != nil {
		t.Fatal(err)
	}
	file.Close()

	if x, _ := fileExists(fileName); !x {
		t.Fatalf("file %v doesn't exist", fileName)
	}

	t.Cleanup(func() {
		os.Remove(fileName)
	})
}

func TestMainProgram(t *testing.T) {
	os.Args = []string{"roaring-migrate",
		"--verbose",
	}
	if realMain() == 0 {
		t.Fatal("should fail and it succeeded")
	}
	os.Args = []string{"roaring-migrate",
		"--verbose",
	}
	if realMain() == 0 {
		t.Fatal("should fail and it succeeded")
	}
	dir, err := ioutil.TempDir("", "backup")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up
	os.Args = []string{"roaring-migrate",
		"--verbose=true",
		"--data-dir=testdata/data-dir/",
		"--backup-dir=" + dir,
	}
	if realMain() == 1 {
		t.Fatal("shouldn't fail")
	}

}
