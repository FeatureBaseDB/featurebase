.PHONY: build check-clean clean build-lattice cover cover-viz default docker docker-build docker-test docker-tag-push generate generate-protoc generate-pql generate-statik gometalinter install install-build-deps install-golangci-lint install-gometalinter install-protoc install-protoc-gen-gofast install-peg install-statik prerelease prerelease-upload release release-build test testv testv-race testvsub testvsub-race  test-txstore-rbf lattice

CLONE_URL=github.com/pilosa/pilosa
MOD_VERSION=v2
VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
LATTICE_COMMIT := $(shell git -C lattice rev-parse --short HEAD 2>/dev/null)
VARIANT = Molecula
VERSION_ID = $(VERSION)-$(GOOS)-$(GOARCH)
BRANCH := $(if $(TRAVIS_BRANCH),$(TRAVIS_BRANCH),$(if $(CIRCLE_BRANCH),$(CIRCLE_BRANCH),$(shell git rev-parse --abbrev-ref HEAD)))
BRANCH_ID := $(BRANCH)-$(GOOS)-$(GOARCH)
BUILD_TIME := $(shell date -u +%FT%T%z)
SHARD_WIDTH = 20
COMMIT := $(shell git describe --exact-match >/dev/null 2>&1 || git rev-parse --short HEAD)
LDFLAGS="-X github.com/pilosa/pilosa/v2.Version=$(VERSION) -X github.com/pilosa/pilosa/v2.BuildTime=$(BUILD_TIME) -X github.com/pilosa/pilosa/v2.Variant=$(VARIANT) -X github.com/pilosa/pilosa/v2.Commit=$(COMMIT) -X github.com/pilosa/pilosa/v2.LatticeCommit=$(LATTICE_COMMIT)"
GO_VERSION=1.16.7
RELEASE ?= 0
RELEASE_ENABLED = $(subst 0,,$(RELEASE))
BUILD_TAGS += $(if $(RELEASE_ENABLED),release)
BUILD_TAGS += shardwidth$(SHARD_WIDTH)
TEST_TAGS = roaringparanoia
define LICENSE_HASH_CODE
    head -13 $1 | sed -e 's/Copyright 20[0-9][0-9]/Copyright 20XX/g' | shasum | cut -f 1 -d " "
endef
LICENSE_HASH=$(shell $(call LICENSE_HASH_CODE, pilosa.go))
UNAME := $(shell uname -s)
ifeq ($(UNAME), Darwin)
    IS_MACOS:=1
else
    IS_MACOS:=0
endif

export GO111MODULE=on
export GOPRIVATE=github.com/molecula
export CGO_ENABLED=0

# Run tests and compile Pilosa
default: test build

# Remove build directories
clean:
	rm -rf vendor build

# Set up vendor directory using `go mod vendor`
vendor: go.mod
	go mod vendor

# Run test suite
test:
	go test ./... -tags='$(BUILD_TAGS) $(TEST_TAGS)' $(TESTFLAGS) -v

# Run test suite with race flag
test-race:
	CGO_ENABLED=1 go test ./... -tags='$(BUILD_TAGS) $(TEST_TAGS)' $(TESTFLAGS) -race -timeout 60m -v

testv: topt testvsub

testv-race: topt-race testvsub-race

# testvsub: run go test -v in sub-directories in "local mode" with incremental output,
#            avoiding go -test ./... "package list mode" which doesn't give output
#            until the test run finishes. Package list mode makes it hard to
#            find which test is hung/deadlocked.
#
testvsub:
	set -e; for i in boltdb ctl http pg pql rbf roaring server sql txkey; do \
           echo; echo "___ testing subpkg $$i"; \
           cd $$i; pwd; \
           go test -tags='$(BUILD_TAGS) $(TEST_TAGS)' $(TESTFLAGS) -v -timeout 60m || break; \
           echo; echo "999 done testing subpkg $$i"; \
           cd ..; \
        done

testvsub-race:
	set -e; for i in boltdb ctl http pg pql rbf roaring server sql txkey; do \
           echo; echo "___ testing subpkg $$i -race"; \
           cd $$i; pwd; \
           CGO_ENABLED=1 go test -tags='$(BUILD_TAGS) $(TEST_TAGS)' $(TESTFLAGS) -v -race -timeout 60m || break; \
           echo; echo "999 done testing subpkg $$i -race"; \
           cd ..; \
        done

tour:
	./tournament.sh

bench:
	go test ./... -bench=. -run=NoneZ -timeout=127m $(TESTFLAGS)

# Run test suite with coverage enabled
cover:
	mkdir -p build
	$(MAKE) test TESTFLAGS="-coverprofile=build/coverage.out"

# Run test suite with coverage enabled and view coverage results in browser
cover-viz: cover
	go tool cover -html=build/coverage.out

# Compile Pilosa
build:
	go build -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) ./cmd/pilosa

# Create a single release build under the build directory
release-build:
	$(MAKE) $(if $(DOCKER_BUILD),docker-)build FLAGS="-o build/pilosa-$(VERSION_ID)/pilosa" RELEASE=1
	cp NOTICE README.md LICENSE build/pilosa-$(VERSION_ID)
	tar -cvz -C build -f build/pilosa-$(VERSION_ID).tar.gz pilosa-$(VERSION_ID)/
	@echo Created release build: build/pilosa-$(VERSION_ID).tar.gz

# Error out if there are untracked changes in Git
check-clean:
ifndef SKIP_CHECK_CLEAN
	$(if $(shell git status --porcelain),$(error Git status is not clean! Please commit or checkout/reset changes.))
endif

# Create release build tarballs for all supported platforms. Linux compilation happens under Docker.
release: check-clean generate-statik
	$(MAKE) release-build GOOS=darwin GOARCH=amd64
	$(MAKE) release-build GOOS=linux GOARCH=amd64 $(if $(IS_MACOS),DOCKER_BUILD=1)

# Create release build tarballs for all supported platforms. Same as `release`, but without embedded Lattice UI.
release-sans-ui: check-clean
	rm -f statik/statik.go
	$(MAKE) release-build GOOS=darwin GOARCH=amd64
	$(MAKE) release-build GOOS=linux GOARCH=amd64 $(if $(IS_MACOS),DOCKER_BUILD=1)

# try (e.g.) internal/clustertests/docker-compose-replication2.yml
DOCKER_COMPOSE=internal/clustertests/docker-compose.yml

# Run cluster integration tests using docker. Requires docker daemon to be
# running. This will catch changes to internal/clustertests/*.go, but if you
# make changes to Pilosa, you'll want to run clustertests-build to rebuild the
# pilosa image.
clustertests: vendor
	docker-compose -f $(DOCKER_COMPOSE) down
	docker-compose -f $(DOCKER_COMPOSE) build client1
	docker-compose -f $(DOCKER_COMPOSE) up --exit-code-from=client1


# Like clustertests, but rebuilds all images.
clustertests-build: vendor
	docker-compose -f $(DOCKER_COMPOSE) down -v
	docker-compose -f $(DOCKER_COMPOSE) up --exit-code-from=client1 --build

# Create prerelease builds
prerelease:
	$(MAKE) release-build GOOS=linux GOARCH=amd64 VERSION_ID=$$\(BRANCH_ID\)
	$(if $(shell git describe --tags --exact-match HEAD),$(MAKE) release)

prerelease-upload:
	aws s3 sync build/ s3://build.pilosa.com/ --exclude "*" --include "*.tar.gz" --acl public-read

# Install Pilosa
install:
	go install -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) ./cmd/pilosa

install-bench:
	go install -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) ./cmd/pilosa-bench

# Ensure lattice is cloned and the pinned version is checked out
lattice:
	git submodule update --init

# Build the lattice assets
build-lattice: lattice require-yarn
	cd lattice && yarn install && yarn build

# Upgrade lattice to the latest version
upgrade-lattice: lattice
	git submodule update --remote

# `go generate` protocol buffers
generate-protoc: require-protoc require-protoc-gen-gofast
	go generate github.com/pilosa/pilosa/v2/internal

# `go generate` statik assets (lattice UI)
generate-statik: build-lattice require-statik
	go generate github.com/pilosa/pilosa/v2/statik

# `go generate` stringers
generate-stringer:
	go generate github.com/pilosa/pilosa/v2

generate-pql: require-peg
	cd pql && peg -inline pql.peg && cd ..

generate-proto-grpc: require-protoc require-protoc-gen-go
	protoc -I proto proto/pilosa.proto --go_out=plugins=grpc:proto
	protoc -I proto proto/vdsm/vdsm.proto --go_out=plugins=grpc:proto
	# TODO: Modify above commands and remove the below mv if possible.
	# See https://go-review.googlesource.com/c/protobuf/+/219298/ for info on --go-opt
	# I couldn't get it to work during development - Cody
	cp -r proto/github.com/pilosa/pilosa/v2/proto/ proto/
	rm -rf proto/github.com

# `go generate` all needed packages
generate: generate-protoc generate-statik generate-stringer generate-pql

# Create Docker image from Dockerfile
docker: vendor
	docker build \
	    --build-arg GO_VERSION=$(GO_VERSION) \
	    --tag pilosa:$(VERSION) .
	@echo Created docker image: pilosa:$(VERSION)

# Tag and push a Docker image
docker-tag-push: vendor
	docker tag "pilosa:$(VERSION)" $(DOCKER_TARGET)
	docker push $(DOCKER_TARGET)
	@echo Pushed docker image: $(DOCKER_TARGET)

docker-release: check-clean lattice
	$(MAKE) docker-build GOOS=linux GOARCH=amd64
	$(MAKE) docker-build GOOS=darwin GOARCH=amd64
	$(MAKE) docker-build GOOS=darwin GOARCH=arm64

# Compile Pilosa inside Docker container
docker-build: vendor
	docker build \
	    --build-arg GO_VERSION=$(GO_VERSION) \
	    --build-arg MAKE_FLAGS="GOOS=$(GOOS) GOARCH=$(GOARCH)" \
	    --target pilosa-builder \
	    --tag pilosa:build .
	docker create --name pilosa-build pilosa:build
	mkdir -p build/pilosa-$(VERSION_ID)
	docker cp pilosa-build:/pilosa/build/. ./build/pilosa-$(VERSION_ID)
	docker rm pilosa-build
	tar -cvz -C build -f build/pilosa-$(VERSION_ID).tar.gz pilosa-$(VERSION_ID)/

# Install diagnostic pilosa-keydump tool. Allows viewing the keys in a transaction-engine directory.
pilosa-keydump:
	go install -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) ./cmd/pilosa-keydump

# Install diagnostic pilosa-chk tool for string translations and fragment checksums.
pilosa-chk:
	go install -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) ./cmd/pilosa-chk

pilosa-fsck:
	cd ./cmd/pilosa-fsck && make install && make release

# Run Pilosa tests inside Docker container
docker-test:
	docker run --rm -v $(PWD):/go/src/$(CLONE_URL) -w /go/src/$(CLONE_URL) golang:$(GO_VERSION) go test -tags='$(BUILD_TAGS) $(TEST_TAGS)' $(TESTFLAGS) ./...

# Must use bash in order to -o pipefail; otherwise the tee will hide red tests.
# run top tests, not subdirs. print summary red/green after.
# The \-\-\- FAIL avoids counting the extra two FAIL strings at then bottom of log.topt.
topt:
	mv log.topt.roar log.topt.roar.prev || true
	$(eval SHELL:=/bin/bash) set -o pipefail; go test -v -tags='$(BUILD_TAGS) $(TEST_TAGS)' $(TESTFLAGS) 2>&1 | tee log.topt.roar
	@echo "   log.topt.roar green: \c"; cat log.topt.roar | grep PASS |wc -l
	@echo "   log.topt.roar   red: \c"; cat log.topt.roar | grep '\-\-\- FAIL' | wc -l

topt-race:
	mv log.topt.race log.topt.race.prev || true
	$(eval SHELL:=/bin/bash) set -o pipefail; CGO_ENABLED=1 go test -race -v -tags='$(BUILD_TAGS) $(TEST_TAGS)' $(TESTFLAGS) 2>&1 | tee log.topt.race
	@echo "   log.topt.race green: \c"; cat log.topt.race | grep PASS |wc -l
	@echo "   log.topt.race   red: \c"; cat log.topt.race | grep '\-\-\- FAIL' | wc -l

# Run golangci-lint
golangci-lint: require-golangci-lint
	golangci-lint run --timeout 3m --skip-files '.*\.peg\.go'

# Alias
linter: golangci-lint

# Better alias
ocd: golangci-lint

# Run gometalinter with custom flags
gometalinter: require-gometalinter vendor
	GO111MODULE=off gometalinter --vendor --disable-all \
	    --deadline=300s \
	    --enable=deadcode \
	    --enable=gochecknoinits \
	    --enable=gofmt \
	    --enable=goimports \
	    --enable=gotype \
	    --enable=gotypex \
	    --enable=ineffassign \
	    --enable=interfacer \
	    --enable=maligned \
	    --enable=misspell \
	    --enable=nakedret \
	    --enable=staticcheck \
	    --enable=unconvert \
	    --enable=unparam \
	    --enable=vet \
	    --exclude "^internal/.*\.pb\.go" \
	    --exclude "^pql/pql.peg.go" \
	    ./...

# Verify that all Go files have license header
check-license-headers: SHELL:=/bin/bash
check-license-headers:
	@! find . -path ./vendor -prune -o -name '*.go' -print | grep -v -F -f license.exceptions | while read fn;\
	    do [[ `$(call LICENSE_HASH_CODE, $$fn)` == $(LICENSE_HASH) ]] || echo $$fn; done | grep '.'

######################
# Build dependencies #
######################

# Verifies that needed build dependency is installed. Errors out if not installed.
require-%:
	$(if $(shell command -v $* 2>/dev/null),\
		$(info Verified build dependency "$*" is installed.),\
		$(error Build dependency "$*" not installed. To install, try `make install-$*`))

install-build-deps: install-protoc-gen-gofast install-protoc install-statik install-stringer install-peg

install-statik:
	go get -u github.com/rakyll/statik

install-stringer:
	GO111MODULE=off go get -u golang.org/x/tools/cmd/stringer

install-protoc-gen-gofast:
	GO111MODULE=off go get -u github.com/gogo/protobuf/protoc-gen-gofast

install-protoc-gen-go:
	GO111MODULE=off go get -u github.com/golang/protobuf/protoc-gen-go

install-protoc:
	@echo This tool cannot automatically install protoc. Please download and install protoc from https://google.github.io/proto-lens/installing-protoc.html

install-peg:
	GO111MODULE=off go get github.com/pointlander/peg

install-golangci-lint:
	GO111MODULE=off go get github.com/golangci/golangci-lint/cmd/golangci-lint

install-gometalinter:
	GO111MODULE=off go get -u github.com/alecthomas/gometalinter
	GO111MODULE=off gometalinter --install
	GO111MODULE=off go get github.com/remyoudompheng/go-misc/deadcode

test-txstore-rbf:
	PILOSA_TXSRC=rbf $(MAKE) testv-race

test-txstore-rbf_bolt:
	PILOSA_TXSRC=rbf_bolt $(MAKE) testv-race

