package internal

import (
	"bytes"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pkg/errors"
)

var FileOrURLNotFound = errors.New("file or url does not exist")

// ReadFileOrURL reads a path from the filesystem or an s3 URL.
// The s3client parameter is required if reading an s3 URL.
// ReadFileOrURL returns FileOrURLNotFound when the local filesystem
// path or remote s3 location is not found.
func ReadFileOrURL(name string, s3client s3iface.S3API) ([]byte, error) {
	var content []byte
	var err error
	if strings.HasPrefix(name, "s3://") {
		if s3client == nil {
			return nil, errors.New("missing s3 client")
		}
		u, err := url.Parse(name)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing S3 URL %v", name)
		}
		bucket := u.Host
		key := u.Path[1:] // strip leading slash

		result, err := s3client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchBucket:
					return nil, FileOrURLNotFound
				case s3.ErrCodeNoSuchKey:
					return nil, FileOrURLNotFound
				}
			}
			return nil, errors.Wrapf(err, "fetching S3 object %v", name)
		}

		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(result.Body)
		if err != nil {
			return nil, err
		}
		content = buf.Bytes()
	} else {
		content, err = os.ReadFile(name)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, FileOrURLNotFound
			}
			return nil, errors.Wrapf(err, "reading file %v", name)
		}
	}
	return content, nil
}

func WriteFileOrURL(name string, contents []byte, s3client s3iface.S3API) error {
	var err error
	if strings.HasPrefix(name, "s3://") {
		if s3client == nil {
			return errors.New("missing s3 client")
		}
		u, err := url.Parse(name)
		if err != nil {
			return errors.Wrapf(err, "parsing S3 URL %v", name)
		}
		bucket := u.Host
		key := u.Path[1:] // strip leading slash

		_, err = s3client.PutObject(&s3.PutObjectInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(contents),
			ContentLength: aws.Int64(int64(len(contents))),
		})
		if err != nil {
			return errors.Wrapf(err, "putting S3 object %v", name)
		}
	} else {
		err = os.WriteFile(name, contents, 0o644)
		if err != nil {
			return errors.Wrapf(err, "reading file %v", name)
		}
	}
	return nil
}
