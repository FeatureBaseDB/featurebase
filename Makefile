.PHONY: vendor

default:

vendor:
	godep save ./...
	cp -r $(GOPATH)/src/github.com/gogo/protobuf/proto/testdata vendor/github.com/gogo/protobuf/proto/testdata

docker:
	docker build -t pilosa:latest .
