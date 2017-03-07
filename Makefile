.PHONY: glide vendor-update docker pilosa pilosactl crossbuild install generate

GLIDE := $(shell command -v glide 2>/dev/null)
VERSION := $(shell git describe --tags)
IDENTIFIER := $(VERSION)-$(GOOS)-$(GOARCH)
PROTOC := $(shell command -v protoc 2> /dev/null)
CLONE_URL=github.com/pilosa/pilosa
BUILD_TIME=`date -u +%FT%T%z`
LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"

default: test pilosa pilosactl

$(GOPATH)/bin:
	mkdir $(GOPATH)/bin

glide: $(GOPATH)/bin
ifndef GLIDE
	curl https://glide.sh/get | sh
endif

vendor: glide glide.yaml
	glide install

glide.lock: glide glide.yaml
	glide update

vendor-update: glide.lock

test: vendor
	go test $(shell cd $(GOPATH)/src/$(CLONE_URL); go list ./... | grep -v vendor)

pilosa: vendor
	go build $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pilosa

pilosactl: vendor
	go build $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pilosactl

crossbuild: vendor
	mkdir -p build/pilosa-$(IDENTIFIER)
	make pilosa FLAGS="-o build/pilosa-$(IDENTIFIER)/pilosa"
	make pilosactl FLAGS="-o build/pilosa-$(IDENTIFIER)/pilosactl"

install: vendor
	go install $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pilosa
	go install $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pilosactl

.protoc-gen-gofast: vendor
ifndef PROTOC
    $(error "protoc is not available please install protoc from https://github.com/google/protobuf/releases")
endif
	go build -o .protoc-gen-gofast ./vendor/github.com/gogo/protobuf/protoc-gen-gofast
	cp ./.protoc-gen-gofast $(GOPATH)/bin/protoc-gen-gofast

generate: .protoc-gen-gofast
	go generate github.com/pilosa/pilosa/internal

docker:
	docker build -t pilosa:latest .
