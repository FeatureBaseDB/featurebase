package bench

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// NewS3Uploader creates an S3Uploader with specified bucket and key
func NewS3Uploader(bucket string, key string) *S3Uploader {
	return &S3Uploader{
		bucket,
		s3.New(session.New(&aws.Config{})),
		key,
	}
}

// S3Uploader is an io.Writer for sending output to AWS S3 storage.
type S3Uploader struct {
	bucket  string
	service *s3.S3
	key     string
}

// Write writes data to the uploader's bucket/key
func (u *S3Uploader) Write(data []byte) (int, error) {
	// first return value of PutObject contains an ETag (hash) of the uploaded object, not needed here
	fmt.Println(string(data))
	_, err := u.service.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(string(data)),
		Bucket: &u.bucket,
		Key:    &u.key,
	})

	return 0, err
}
