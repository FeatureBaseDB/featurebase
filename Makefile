.PHONY: build check-clean clean cover cover-viz default docker docker-build docker-test generate generate-protoc install install-build-deps install-dep install-protoc install-protoc-gen-gofast prerelease prerelease-build prerelease-upload release release-build require-dep require-protoc require-protoc-gen-gofast test

CLONE_URL=github.com/pilosa/pilosa
VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
VERSION_ID = $(if $(ENTERPRISE_ENABLED),enterprise-)$(VERSION)-$(GOOS)-$(GOARCH)
BRANCH := $(if $(TRAVIS_BRANCH),$(TRAVIS_BRANCH),$(shell git rev-parse --abbrev-ref HEAD))
BRANCH_ID := $(BRANCH)-$(GOOS)-$(GOARCH)
BUILD_TIME := $(shell date -u +%FT%T%z)
LDFLAGS="-X github.com/pilosa/pilosa.Version=$(VERSION) -X github.com/pilosa/pilosa.BuildTime=$(BUILD_TIME) -X github.com/pilosa/pilosa.Enterprise=$(if $(ENTERPRISE_ENABLED),1)"
GO_VERSION=latest
ENTERPRISE ?= 0
ENTERPRISE_ENABLED = $(subst 0,,$(ENTERPRISE))
RELEASE ?= 0
RELEASE_ENABLED = $(subst 0,,$(RELEASE))
BUILD_TAGS += $(if $(ENTERPRISE_ENABLED),enterprise)
BUILD_TAGS += $(if $(RELEASE_ENABLED),release)

# Run tests and compile Pilosa
default: test build

# Remove vendor and build directories
clean:
	rm -rf vendor build

# Set up vendor directory using `dep`
vendor: Gopkg.toml
	$(MAKE) require-dep
	dep ensure
	touch vendor

# Run test suite
test: vendor
	go test ./... -tags='$(BUILD_TAGS)' $(TESTFLAGS)

# Run test suite with coverage enabled
cover: vendor
	mkdir -p build
	$(MAKE) test TESTFLAGS="-coverprofile=build/coverage.out"

# Run test suite with coverage enabled and view coverage results in browser
cover-viz: cover
	go tool cover -html=build/coverage.out

# Compile Pilosa
build: vendor
	go build -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) ./cmd/pilosa

# Create a single release build under the build directory
release-build: vendor
	$(MAKE) $(if $(DOCKER_BUILD),docker-)build FLAGS="-o build/pilosa-$(VERSION_ID)/pilosa" RELEASE=1
	cp NOTICE LICENSE README.md build/pilosa-$(VERSION_ID)
	$(if $(ENTERPRISE_ENABLED),cp enterprise/COPYING build/pilosa-$(VERSION_ID))
	tar -cvz -C build -f build/pilosa-$(VERSION_ID).tar.gz pilosa-$(VERSION_ID)/
	@echo Created release build: build/pilosa-$(VERSION_ID).tar.gz

# Error out if there are untracked changes in Git
check-clean:
ifndef SKIP_CHECK_CLEAN
	$(if $(shell git status --porcelain),$(error Git status is not clean! Please commit or checkout/reset changes.))
endif

# Create release build tarballs for all supported platforms. Linux compilation happens under Docker.
release: check-clean
	$(MAKE) release-build GOOS=darwin GOARCH=amd64
	$(MAKE) release-build GOOS=darwin GOARCH=amd64 ENTERPRISE=1
	$(MAKE) release-build GOOS=linux GOARCH=amd64 DOCKER_BUILD=1
	$(MAKE) release-build GOOS=linux GOARCH=amd64 DOCKER_BUILD=1 ENTERPRISE=1
	$(MAKE) release-build GOOS=linux GOARCH=386 DOCKER_BUILD=1
	$(MAKE) release-build GOOS=linux GOARCH=386 DOCKER_BUILD=1 ENTERPRISE=1

# Create branch-tagged pre-release for client library CI jobs
prerelease-build: vendor
	$(MAKE) release-build VERSION_ID=$(BRANCH_ID)

# Create prerelease build for Linux/amd64
prerelease:
	$(MAKE) prerelease-build GOOS=linux GOARCH=amd64

# Upload prerelease to S3
prerelease-upload: prerelease
	aws s3 cp build/pilosa-$(BRANCH_ID).tar.gz s3://build.pilosa.com/pilosa-$(BRANCH_ID).tar.gz --acl public-read

# Install Pilosa
install: vendor
	go install -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) ./cmd/pilosa

# `go generate` protocol buffers
generate-protoc: require-protoc require-protoc-gen-gofast
	go generate github.com/pilosa/pilosa/internal

# `go generate` stringers
generate-stringer:
	go generate github.com/pilosa/pilosa

# `go generate` all needed packages
generate: generate-protoc generate-stringer

# Create Docker image from Dockerfile
docker:
	docker build -t "pilosa:$(VERSION)" .
	@echo Created docker image: pilosa:$(VERSION)

# Compile Pilosa inside Docker container
docker-build:
	docker run --rm -v $(PWD):/go/src/$(CLONE_URL) -w /go/src/$(CLONE_URL) -e GOOS=$(GOOS) -e GOARCH=$(GOARCH) golang:$(GO_VERSION) go build -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pilosa

# Run Pilosa tests inside Docker container
docker-test:
	docker run --rm -v $(PWD):/go/src/$(CLONE_URL) -w /go/src/$(CLONE_URL) golang:$(GO_VERSION) go test -tags='$(BUILD_TAGS)' $(TESTFLAGS) ./...

######################
# Build dependencies #
######################

# Verifies that needed build dependency is installed. Errors out if not installed.
define require
	$(if $(shell command -v $1 2>/dev/null),
		$(info Verified build dependency "$1" is installed.),
		$(error Build dependency "$1" not installed. To install, run `make install-$1` or `make install-build-deps`))
endef

require-dep:
	$(call require,dep)

require-protoc-gen-gofast:
	$(call require,protoc-gen-gofast)

require-protoc:
	$(call require,protoc)

install-build-deps: install-dep install-protoc-gen-gofast install-protoc install-stringer

install-dep:
	go get -u github.com/golang/dep/cmd/dep

install-stringer:
	go get -u golang.org/x/tools/cmd/stringer

install-protoc-gen-gofast:
	go get -u github.com/gogo/protobuf/protoc-gen-gofast

install-protoc:
	@echo This tool cannot automatically install protoc. Please download and install protoc from https://google.github.io/proto-lens/installing-protoc.html
