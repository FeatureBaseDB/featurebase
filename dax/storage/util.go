package storage

import "io"

// trackingReader wraps a Reader and calls an custom "update" function
// whenever Read is called. Used by the storage layer to keep track of
// how much of the writelog has been read.
type trackingReader struct {
	r      io.Reader
	update func(int, error)
}

func (tr *trackingReader) Read(p []byte) (n int, err error) {
	n, err = tr.r.Read(p)
	tr.update(n, err)
	return n, err
}

func (tr *trackingReader) Close() error {
	if closer, ok := tr.r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
