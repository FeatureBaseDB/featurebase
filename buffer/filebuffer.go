package buffer

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

// NewFileBuffer returns a file buffer which will use an in-memory buffer, until `max` bytes have been written, at which point it will write the contents of memory to a file, and continue writing future data to the file.
// The file will be written to `temp` directory. The buffer fulfills the io.Reader and io.Writer interface
func NewFileBuffer(max int, temp string) *FileBuffer {
	return &FileBuffer{max: max, tempDir: temp}
}

type FileBuffer struct {
	max     int
	buf     bytes.Buffer
	file    *os.File
	tempDir string
	reading bool
	files   []*os.File
	mu      sync.Mutex
}

func (fb *FileBuffer) Write(p []byte) (n int, err error) {
	if fb.reading {
		panic("cannot write after read")
	}
	if fb.file != nil {
		return fb.file.Write(p)
	}
	n, err = fb.buf.Write(p)
	if err != nil {
		return
	}
	if fb.buf.Len() > fb.max {
		fb.file, err = ioutil.TempFile(fb.tempDir, "filebuffer-")
		if err != nil {
			return
		}
		_, err = io.Copy(fb.file, &fb.buf)
		fb.buf.Reset()
	}
	return
}

func (fb *FileBuffer) Len() (int64, error) {
	if fb.file == nil {
		return int64(fb.buf.Len()), nil
	}
	fi, err := fb.file.Stat()
	if err != nil {
		return 0, err
	}

	return fi.Size(), nil
}

func (fb *FileBuffer) Read(p []byte) (n int, err error) {
	if fb.file != nil {
		if !fb.reading {
			fb.reading = true
			_, err = fb.file.Seek(0, 0)
			if err != nil {
				return
			}
		}
		return fb.file.Read(p)
	}
	fb.reading = true
	return fb.buf.Read(p)
}

func (fb *FileBuffer) Close() error {
	if fb.file != nil {
		name := fb.file.Name()
		if err := fb.file.Close(); err != nil {
			return err
		}
		for _, f := range fb.files {
			f.Close()
		}
		fb.files = fb.files[:0]
		fb.file = nil
		return os.Remove(name)
	}
	return nil
}

func (fb *FileBuffer) Reset() error {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	fb.reading = false
	fb.buf.Reset()
	return fb.Close()
}

func (fb *FileBuffer) NewReader() (io.Reader, error) {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	fb.reading = true
	if fb.file == nil {
		return bytes.NewReader(fb.buf.Bytes()), nil
	}
	f, err := os.OpenFile(fb.file.Name(), os.O_RDONLY, 0)
	fb.files = append(fb.files, f)
	return f, err
}
