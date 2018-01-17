.PHONY: dep docker pilosa release-build prerelease-build release prerelease prerelease-upload install generate statik test cover cover-pkg cover-viz clean docker-build docker-test

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
	mkdir -p build/coverage
	echo "mode: set" > build/coverage/all.out
	for pkg in $(PKGS) ; do \
		make cover-pkg PKG=$$pkg ; \
	done

cover-pkg:
	mkdir -p build/coverage
	touch build/coverage/$(subst /,-,$(PKG)).out
	go test -coverprofile=build/coverage/$(subst /,-,$(PKG)).out $(PKG)
	tail -n +2 build/coverage/$(subst /,-,$(PKG)).out >> build/coverage/all.out

cover-viz: cover
	go tool cover -html=build/coverage/all.out

pilosa: vendor
	go build -ldflags $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pilosa

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
	make pilosa FLAGS="-o build/pilosa-master-$(GOOS)-$(GOARCH)/pilosa"
	cp LICENSE README.md build/pilosa-master-$(GOOS)-$(GOARCH)
	tar -cvz -C build -f build/pilosa-master-$(GOOS)-$(GOARCH).tar.gz pilosa-master-$(GOOS)-$(GOARCH)/
	@echo "Created pre-release build: build/pilosa-master-$(GOOS)-$(GOARCH).tar.gz"

prerelease:
	make prerelease-build GOOS=linux GOARCH=amd64

prerelease-upload: prerelease
	aws s3 cp build/pilosa-master-linux-amd64.tar.gz s3://build.pilosa.com/pilosa-master-linux-amd64.tar.gz --acl public-read

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
	go generate github.com/pilosa/pilosa

generate: generate-protoc generate-statik

statik:
ifndef STATIK
	go get github.com/rakyll/statik
endif

docker:
	docker build -t "pilosa:$(VERSION)" --build-arg ldflags=$(LDFLAGS) .
	@echo "Created image: pilosa:$(VERSION)"

docker-build:
	docker run --rm -v $(PWD):/go/src/$(CLONE_URL) -w /go/src/$(CLONE_URL) -e GOOS=$(GOOS) -e GOARCH=$(GOARCH) $(DOCKER_GOLANG_IMAGE) go build -ldflags $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pilosa

docker-test:
	docker run --rm -v $(PWD):/go/src/$(CLONE_URL) -w /go/src/$(CLONE_URL) $(DOCKER_GOLANG_IMAGE) go test $(TESTFLAGS) $(PKGS)
