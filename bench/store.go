package bench

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// NewS3Uploader creates an S3Uploader with a hardcoded region and bucket,
// and a specified key.
func NewS3Uploader(key string) *S3Uploader {
	region := "us-east-1"
	bucket := "benchmarks-pilosa"
	return &S3Uploader{
		region,
		bucket,
		s3.New(session.New(&aws.Config{Region: aws.String(region)})),
		key,
	}
}

// S3Uploader is an io.Writer for sending output to AWS S3 storage.
type S3Uploader struct {
	region  string
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
