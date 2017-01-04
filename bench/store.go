package bench

import (
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Uploader interface {
	Upload(name string, data string) error
}

func NewS3Uploader() *S3Uploader {
	region := "us-east-1"
	return &S3Uploader{
		region,
		"benchmarks-pilosa",
		s3.New(session.New(&aws.Config{Region: aws.String(region)})),
	}
}

type S3Uploader struct {
	region  string
	bucket  string
	service *s3.S3
}

func (u *S3Uploader) Upload(key, data string) error {
	_, err := u.service.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(data),
		Bucket: &u.bucket,
		Key:    &key,
	})

	return err
}

// pilosa-sandbox is "us-east-1"
// benchmarks go to benchmarks-pilosa/run-uuid/??.txt
// first return value of PutObject contains an ETag (hash) of the uploaded object, returned here but not needed
func UploadToS3BucketSimple(region, bucket, key, data string) (*s3.PutObjectOutput, error) {
	svc := s3.New(session.New(&aws.Config{Region: aws.String(region)}))

	uploadResult, err := svc.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(data),
		Bucket: &bucket,
		Key:    &key,
	})

	if err != nil {
		log.Printf("Failed to upload to s3:%s/%s, %s\n", bucket, key, err)
		return nil, err
	}
	// log.Printf("Successfully uploaded to s3:%s/%s\n", bucket, key)

	return uploadResult, err
}
