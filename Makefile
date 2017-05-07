.PHONY: glide vendor-update Dockerfile docker pilosa crossbuild install generate statik

GLIDE := $(shell command -v glide 2>/dev/null)
STATIK := $(shell command -v statik 2>/dev/null)
PROTOC := $(shell command -v protoc 2>/dev/null)
VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
IDENTIFIER := $(VERSION)-$(GOOS)-$(GOARCH)
CLONE_URL=github.com/pilosa/pilosa
BUILD_TIME=`date -u +%FT%T%z`
LDFLAGS=-ldflags "-X github.com/pilosa/pilosa/cmd.Version=$(VERSION) -X github.com/pilosa/pilosa/cmd.BuildTime=$(BUILD_TIME)"

default: test pilosa

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

crossbuild: vendor
	mkdir -p build/pilosa-$(IDENTIFIER)
	make pilosa FLAGS="-o build/pilosa-$(IDENTIFIER)/pilosa"

install: vendor
	go install $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pilosa

.protoc-gen-gofast: vendor
ifndef PROTOC
	$(error "protoc is not available. please install protoc from https://github.com/google/protobuf/releases")
endif
	go build -o .protoc-gen-gofast ./vendor/github.com/gogo/protobuf/protoc-gen-gofast
	cp ./.protoc-gen-gofast $(GOPATH)/bin/protoc-gen-gofast

generate-protoc: .protoc-gen-gofast
	go generate github.com/pilosa/pilosa/internal

generate-statik: statik
	go generate github.com/pilosa/pilosa

generate: generate-protoc generate-statik

statik:
ifndef STATIK
	go get github.com/rakyll/statik
endif

Dockerfile:
	@echo Creating Dockerfile for pilosa:$(VERSION)...
	@echo 'FROM scratch' > $@
	@echo 'MAINTAINER Pilosa Corp. <dev@pilosa.com>' >> $@
	@echo 'EXPOSE 10101' >> $@
	@echo 'VOLUME /data' >> $@
	@echo 'ADD build/pilosa-$(VERSION)-linux-amd64/pilosa /pilosa' >> $@
	@echo 'ENTRYPOINT ["/pilosa"]' >> $@
	@echo 'CMD ["server", "--data-dir", "/data"]' >> $@

docker: Dockerfile
	make crossbuild GOOS=linux GOARCH=amd64
	docker build -t "pilosa:$(VERSION)" .
	@echo "Created image: pilosa:$(VERSION)"
