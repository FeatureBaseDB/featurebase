package mock

import "sync"

type ReadCloser struct {
	ReadFunc  func(p []byte) (int, error)
	CloseFunc func() error
	once      sync.Once
}

func (rc *ReadCloser) Read(p []byte) (int, error) {
	return rc.ReadFunc(p)
}

func (rc *ReadCloser) Close() error {
	var err error = nil
	rc.once.Do(func() {
		err = rc.CloseFunc()
	})
	return err
}
