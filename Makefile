.PHONY: vendor

default:

vendor:
	godep save ./...
	cp -r $(GOPATH)/src/github.com/gogo/protobuf/proto/testdata vendor/github.com/gogo/protobuf/proto/testdata

docker:
	CGO_ENABLED=0 go build github.com/pilosa/pilosa/cmd/pilosa && docker build -t pilosa:latest .
