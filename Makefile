.PHONY: build check-clean clean build-lattice cover cover-viz default docker docker-build docker-test docker-tag-push generate generate-protoc generate-pql generate-statik gometalinter install install-build-deps install-golangci-lint install-gometalinter install-protoc install-protoc-gen-gofast install-peg install-statik prerelease prerelease-upload release release-build require-statik test testv testv-race testvsub testvsub-race

CLONE_URL=github.com/pilosa/pilosa
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
GO_VERSION=latest
RELEASE ?= 0
RELEASE_ENABLED = $(subst 0,,$(RELEASE))
NOCHECKPTR=$(shell go version | grep -q 'go1.1[4,5,6,7]' && echo \"-gcflags=all=-d=checkptr=0\" )
BUILD_TAGS += $(if $(RELEASE_ENABLED),release)
BUILD_TAGS += shardwidth$(SHARD_WIDTH)
define LICENSE_HASH_CODE
    head -13 $1 | sed -e 's/Copyright 20[0-9][0-9]/Copyright 20XX/g' | shasum | cut -f 1 -d " "
endef
LICENSE_HASH=$(shell $(call LICENSE_HASH_CODE, pilosa.go))

export GO111MODULE=on
export GOPRIVATE=github.com/molecula

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
	go test ./... -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)

# Run test suite with race flag
test-race:
	go test ./... -tags='$(BUILD_TAGS)' $(TESTFLAGS) -race $(NOCHECKPTR) -timeout 60m -v

testv: topt testvsub

testv-race: topt-race testvsub-race

# testvsub: run go test -v in sub-directories in "local mode" with incremental output,
#            avoiding go -test ./... "package list mode" which doesn't give output
#            until the test run finishes. Package list mode makes it hard to
#            find which test is hung/deadlocked.
#
testvsub:
	set -e; for i in ctl http pg pql rbf roaring server sql txkey; do \
           echo; echo "___ testing subpkg $$i"; \
           cd $$i; pwd; \
           go test -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR) -v || break; \
           echo; echo "999 done testing subpkg $$i"; \
           cd ..; \
        done

testvsub-race:
	set -e; for i in ctl http pg pql rbf roaring server sql txkey; do \
           echo; echo "___ testing subpkg $$i -race"; \
           cd $$i; pwd; \
           go test -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR) -v -race || break; \
           echo; echo "999 done testing subpkg $$i -race"; \
           cd ..; \
        done

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
release: check-clean
	$(MAKE) release-build GOOS=darwin GOARCH=amd64
	$(MAKE) release-build GOOS=linux GOARCH=amd64


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

lattice:
	git clone git@github.com:molecula/lattice.git

build-lattice: lattice require-yarn
	cd lattice && git pull && yarn install && yarn build

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

# dunno if protoc-gen-gofast is actually needed here
generate-proto-grpc: require-protoc require-protoc-gen-gofast
	protoc -I proto proto/pilosa.proto --go_out=plugins=grpc:proto

# `go generate` all needed packages
generate: generate-protoc generate-statik generate-stringer generate-pql

# Create Docker image from Dockerfile
docker: vendor
	docker build --build-arg BUILD_FLAGS="${FLAGS}" -t "pilosa:$(VERSION)" .
	@echo Created docker image: pilosa:$(VERSION)

# Tag and push a Docker image
docker-tag-push: vendor
	docker tag "pilosa:$(VERSION)" $(DOCKER_TARGET)
	docker push $(DOCKER_TARGET)
	@echo Pushed docker image: $(DOCKER_TARGET)

# Compile Pilosa inside Docker container
docker-build:
	docker run --rm -v $(PWD):/go/src/$(CLONE_URL) -w /go/src/$(CLONE_URL) -e GOOS=$(GOOS) -e GOARCH=$(GOARCH) golang:$(GO_VERSION) go build -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pilosa

# Run Pilosa tests inside Docker container
docker-test:
	docker run --rm -v $(PWD):/go/src/$(CLONE_URL) -w /go/src/$(CLONE_URL) golang:$(GO_VERSION) go test -tags='$(BUILD_TAGS)' $(TESTFLAGS) ./...

# run top tests, not subdirs. print summary red/green after.
# The \-\-\- FAIL avoids counting the extra two FAIL strings at then bottom of log.topt.
topt:
	mv log.topt.roar log.topt.roar.prev || true
	go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.topt.roar
	@echo "   log.topt.roar green: \c"; cat log.topt.roar | grep PASS |wc -l
	@echo "   log.topt.roar   red: \c"; cat log.topt.roar | grep '\-\-\- FAIL' |wc -l

topt-badger:
	mv log.topt.badger log.topt.badger.prev || true
	PILOSA_TXSRC=badger go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.topt.badger
	@echo "   log.topt.badger green: \c"; cat log.topt.badger | grep PASS |wc -l
	@echo "   log.topt.badger   red: \c"; cat log.topt.badger | grep '\-\-\- FAIL' |wc -l

topt-badger-race:
	mv log.topt.badger-race log.topt.badger-race.prev || true
	PILOSA_TXSRC=badger go test -race -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.topt.badger-race
	@echo "   log.topt.badger-race green: \c"; cat log.topt.badger-race | grep PASS |wc -l
	@echo "   log.topt.badger-race   red: \c"; cat log.topt.badger-race | grep '\-\-\- FAIL' |wc -l

topt-rbf:
	mv log.topt.rbf log.topt.rbf.prev || true
	PILOSA_TXSRC=rbf go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.topt.rbf
	@echo "   log.topt.rbf green: \c"; cat log.topt.rbf | grep PASS |wc -l
	@echo "   log.topt.rbf   red: \c"; cat log.topt.rbf | grep '\-\-\- FAIL' |wc -l

topt-rbf-race:
	mv log.topt.rbf-race log.topt.rbf-race.prev || true
	PILOSA_TXSRC=rbf go test -race -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.topt.rbf-race
	@echo "   log.topt.rbf-race green: \c"; cat log.topt.rbf-race | grep PASS |wc -l
	@echo "   log.topt.rbf-race   red: \c"; cat log.topt.rbf-race | grep '\-\-\- FAIL' |wc -l

topt-lmdb:
	mv log.topt.lmdb log.topt.lmdb.prev || true
	PILOSA_TXSRC=lmdb go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.topt.lmdb
	@echo "   log.topt.lmdb green: \c"; cat log.topt.lmdb | grep PASS |wc -l
	@echo "   log.topt.lmdb   red: \c"; cat log.topt.lmdb | grep '\-\-\- FAIL' |wc -l

topt-lmdb-race:
	mv log.topt.lmdb log.topt.lmdb.prev || true
	PILOSA_TXSRC=lmdb go test -race -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.topt.lmdb-race
	@echo "   log.topt.lmdb-race green: \c"; cat log.topt.lmdb-race | grep PASS |wc -l
	@echo "   log.topt.lmdb-race   red: \c"; cat log.topt.lmdb-race | grep '\-\-\- FAIL' |wc -l

topt-race:
	mv log.topt.race log.topt.race.prev || true
	go test -race -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.topt.race
	@echo "   log.topt.race green: \c"; cat log.topt.race | grep PASS |wc -l
	@echo "   log.topt.race   red: \c"; cat log.topt.race | grep '\-\-\- FAIL' |wc -l

# blue-green checks. These run two different storage engines (rbf, roaring, or badger)
# and compare each transaction for a result.

bg-rr: # shorthand for bluegreen test with A:badger; B:roaring
	mv log.bg-rr log.bg-rr.prev || true
	PILOSA_TXSRC=badger_roaring go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.bg-rr
	@echo "   log.bg-rr green: \c"; cat log.bg-rr | grep PASS |wc -l
	@echo "   log.bg-rr   red: \c"; cat log.bg-rr | grep '\-\-\- FAIL' |wc -l

rr-bg: # bluegreen with A:roaring; B:badger (B's values are returned).
	mv log.bg.roar_bg log.bg.roar_bg.prev || true
	PILOSA_TXSRC=roaring_badger go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.rr-bg
	@echo "   log.rr-bg green: \c"; cat log.rr-bg | grep PASS |wc -l
	@echo "   log.rr-bg   red: \c"; cat log.rr-bg | grep '\-\-\- FAIL' |wc -l

rbf-rr:
	mv log.rbf-rr log.rbf-rr.prev || true
	PILOSA_TXSRC=rbf_roaring go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.rbf-rr
	@echo "   log.rbf-rr green: \c"; cat log.rbf-rr | grep PASS |wc -l
	@echo "   log.rbf-rr   red: \c"; cat log.rbf-rr | grep '\-\-\- FAIL' |wc -l

rr-rbf:
	mv log.rr-rbf log.rr-rbf.prev || true
	PILOSA_TXSRC=roaring_rbf go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.rr-rbf
	@echo "   log.rr-rbf green: \c"; cat log.rr-rbf | grep PASS |wc -l
	@echo "   log.rr-rbf   red: \c"; cat log.rr-rbf | grep '\-\-\- FAIL' |wc -l

rbf-bg:
	mv log.rbf-bg log.rbf-bg.prev || true
	PILOSA_TXSRC=rbf_badger go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.rbf-bg
	@echo "   log.rbf-bg green: \c"; cat log.rbf-bg | grep PASS |wc -l
	@echo "   log.rbf-bg   red: \c"; cat log.rbf-bg | grep '\-\-\- FAIL' |wc -l

bg-rbf:
	mv log.bg-rbf log.bg-rbf.prev || true
	PILOSA_TXSRC=badger_rbf go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.bg-rbf
	@echo "   log.bg-rbf green: \c"; cat log.bg-rbf | grep PASS |wc -l
	@echo "   log.bg-rbf   red: \c"; cat log.bg-rbf | grep '\-\-\- FAIL' |wc -l

rbf-lm:
	mv log.rbf-lm log.rbf-lm.prev || true
	PILOSA_TXSRC=rbf_badger go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.rbf-lm
	@echo "   log.rbf-lm green: \c"; cat log.rbf-lm | grep PASS |wc -l
	@echo "   log.rbf-lm   red: \c"; cat log.rbf-lm | grep '\-\-\- FAIL' |wc -l

lm-rbf:
	mv log.lm-rbf log.lm-rbf.prev || true
	PILOSA_TXSRC=badger_rbf go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.lm-rbf
	@echo "   log.lm-rbf green: \c"; cat log.lm-rbf | grep PASS |wc -l
	@echo "   log.lm-rbf   red: \c"; cat log.lm-rbf | grep '\-\-\- FAIL' |wc -l

lm-rr:
	mv log.lm-rr log.lm-rr.prev || true
	PILOSA_TXSRC=lmdb_roaring go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.lm-rr
	@echo "   log.lm-rr green: \c"; cat log.lm-rr | grep PASS |wc -l
	@echo "   log.lm-rr   red: \c"; cat log.lm-rr | grep '\-\-\- FAIL' |wc -l

rr-lm:
	mv log.rr-lm log.rr-lm.prev || true
	PILOSA_TXSRC=roaring_lmdb go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.rr-lm
	@echo "   log.rr-lm green: \c"; cat log.rr-lm | grep PASS |wc -l
	@echo "   log.rr-lm   red: \c"; cat log.rr-lm | grep '\-\-\- FAIL' |wc -l

bg-lm:
	mv log.topt.bg-lm log.topt.bg-lm.prev || true
	PILOSA_TXSRC=badger_lmdb go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.topt.bg-lm
	@echo "   log.topt.bg-lm green: \c"; cat log.topt.bg-lm | grep PASS |wc -l
	@echo "   log.topt.bg-lm   red: \c"; cat log.topt.bg-lm | grep '\-\-\- FAIL' |wc -l

lm-bg:
	mv log.topt.lm-bg log.topt.lm-bg.prev || true
	PILOSA_TXSRC=lmdb_badger go test -v -tags='$(BUILD_TAGS)' $(TESTFLAGS) $(NOCHECKPTR)  2>&1 | tee log.topt.lm-bg
	@echo "   log.topt.lm-bg green: \c"; cat log.topt.lm-bg | grep PASS |wc -l
	@echo "   log.topt.lm-bg   red: \c"; cat log.topt.lm-bg | grep '\-\-\- FAIL' |wc -l


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
