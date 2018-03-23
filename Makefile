.PHONY: dep docker pilosa release-build prerelease-build release prerelease prerelease-upload install generate generate-statik generate-protoc statik test cover cover-pkg cover-viz clean docker-build docker-test

DEP := $(shell command -v dep 2>/dev/null)
STATIK := $(shell command -v statik 2>/dev/null)
PROTOC := $(shell command -v protoc 2>/dev/null)
VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
STATUS := $(shell git status --porcelain)
IDENTIFIER := $(VERSION)-$(GOOS)-$(GOARCH)
CLONE_URL=github.com/pilosa/pilosa
PKGS := $(shell cd $(GOPATH)/src/$(CLONE_URL); go list ./... | grep -v vendor)
BUILD_TIME=`date -u +%FT%T%z`
LDFLAGS="-X github.com/pilosa/pilosa.Version=$(VERSION) -X github.com/pilosa/pilosa.BuildTime=$(BUILD_TIME)"
DOCKER_GOLANG_IMAGE=golang:latest
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
BRANCH := $(if $(TRAVIS_BRANCH),$(TRAVIS_BRANCH),$(GIT_BRANCH))
BRANCH_IDENTIFIER := $(BRANCH)-$(GOOS)-$(GOARCH)

default: test pilosa

clean:
	rm -rf vendor build

$(GOPATH)/bin:
	mkdir $(GOPATH)/bin

dep: $(GOPATH)/bin
	go get -u github.com/golang/dep/cmd/dep

vendor: Gopkg.toml
ifndef DEP
	make dep
endif
	dep ensure
	touch vendor

Gopkg.lock: dep Gopkg.toml
	dep ensure

test: vendor
	go test $(PKGS) $(TESTFLAGS)

cover: vendor
	make test TESTFLAGS="-coverprofile=build/coverage.out"

cover-viz: cover
	go tool cover -html=build/coverage.out

pilosa: vendor
	go build -tags release -ldflags $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pilosa

release-build: vendor
ifdef DOCKER_BUILD
	make docker-build FLAGS="-o build/pilosa-$(IDENTIFIER)/pilosa"
else
	make pilosa FLAGS="-o build/pilosa-$(IDENTIFIER)/pilosa"
endif
	cp LICENSE README.md build/pilosa-$(IDENTIFIER)
	tar -cvz -C build -f build/pilosa-$(IDENTIFIER).tar.gz pilosa-$(IDENTIFIER)/
	@echo "Created release build: build/pilosa-$(IDENTIFIER).tar.gz"

release:
ifeq ($(STATUS),"")
	make release-build GOOS=darwin GOARCH=amd64
	make release-build GOOS=linux GOARCH=amd64 DOCKER_BUILD=1
	make release-build GOOS=linux GOARCH=386 DOCKER_BUILD=1
else
	@echo "Will not create release with unclean git status."
endif

prerelease-build: vendor
	make pilosa FLAGS="-o build/pilosa-$(BRANCH_IDENTIFIER)/pilosa"
	cp LICENSE README.md build/pilosa-$(BRANCH_IDENTIFIER)
	tar -cvz -C build -f build/pilosa-$(BRANCH_IDENTIFIER).tar.gz pilosa-$(BRANCH_IDENTIFIER)/
	@echo "Created pre-release build: build/pilosa-$(BRANCH_IDENTIFIER).tar.gz"

prerelease:
	make prerelease-build GOOS=linux GOARCH=amd64

prerelease-upload: prerelease
	aws s3 cp build/pilosa-$(BRANCH_IDENTIFIER).tar.gz s3://build.pilosa.com/pilosa-$(BRANCH_IDENTIFIER).tar.gz --acl public-read

install: vendor
	go install -ldflags $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pilosa

.protoc-gen-gofast: vendor
ifndef PROTOC
	$(error "protoc is not available. please install protoc from https://github.com/google/protobuf/releases")
endif
	go build -o .protoc-gen-gofast ./vendor/github.com/gogo/protobuf/protoc-gen-gofast
	cp ./.protoc-gen-gofast $(GOPATH)/bin/protoc-gen-gofast

generate-protoc: .protoc-gen-gofast
	go generate github.com/pilosa/pilosa/internal

generate-statik: statik
	go generate github.com/pilosa/pilosa/statik

generate: generate-protoc generate-statik

statik:
ifndef STATIK
	go get github.com/rakyll/statik
endif

docker:
	docker build -t "pilosa:$(VERSION)" --build-arg ldflags=$(LDFLAGS) .
	@echo "Created image: pilosa:$(VERSION)"

docker-build:
	docker run --rm -v $(PWD):/go/src/$(CLONE_URL) -w /go/src/$(CLONE_URL) -e GOOS=$(GOOS) -e GOARCH=$(GOARCH) $(DOCKER_GOLANG_IMAGE) go build -tags release -ldflags $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pilosa

docker-test:
	docker run --rm -v $(PWD):/go/src/$(CLONE_URL) -w /go/src/$(CLONE_URL) $(DOCKER_GOLANG_IMAGE) go test $(TESTFLAGS) $(PKGS)
