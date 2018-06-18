package mock

type ReadCloser struct {
	ReadFunc  func(p []byte) (int, error)
	CloseFunc func() error
}

func (rc *ReadCloser) Read(p []byte) (int, error) {
	return rc.ReadFunc(p)
}

func (rc *ReadCloser) Close() error {
	return rc.CloseFunc()
}
