package bench

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Uploader interface {
	Write(p []byte) (n int, err error)
}

func NewS3Uploader(key string) *S3Uploader {
	region := "us-east-1"
	return &S3Uploader{
		region,
		"benchmarks-pilosa",
		s3.New(session.New(&aws.Config{Region: aws.String(region)})),
		key,
	}
}

type S3Uploader struct {
	region  string
	bucket  string
	service *s3.S3
	key     string
}

// pilosa-sandbox is "us-east-1"
// benchmarks go to benchmarks-pilosa/run-uuid/??.txt
// first return value of PutObject contains an ETag (hash) of the uploaded object, returned here but not needed
func (u *S3Uploader) Write(data []byte) (int, error) {
	fmt.Println(string(data))
	_, err := u.service.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(string(data)),
		Bucket: &u.bucket,
		Key:    &u.key,
	})

	return 0, err
}
