package util

import (
	"io"
	"os"
	"pilosa/config"
	"strings"

	"github.com/kr/s3/s3util"
)

func setup_storage() {
	access_key := config.GetString("AWS_ACCESS_KEY_ID")
	secret := config.GetString("AWS_SECRET_ACCESS_KEY")

	s3util.DefaultConfig.AccessKey = access_key
	s3util.DefaultConfig.SecretKey = secret
}

func Open(s string) (io.ReadCloser, error) {
	if isURL(s) {
		return s3util.Open(s, nil)
	}
	return os.Open(s)
}

func Create(s string) (io.WriteCloser, error) {
	if isURL(s) {
		return s3util.Create(s, nil, nil)
	}
	return os.Create(s)
}

func isURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}
