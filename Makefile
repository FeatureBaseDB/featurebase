.PHONY: build clean build-lattice cover cover-viz default docker docker-build docker-tag-push generate generate-protoc generate-pql generate-statik generate-stringer install install-protoc-gen-gofast install-protoc install-statik install-peg test docker-login

SHELL := /bin/bash
VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
VARIANT = Molecula
GO=go
GOOS=$(shell $(GO) env GOOS)
GOARCH=$(shell $(GO) env GOARCH)
VERSION_ID=$(if $(TRIAL_DEADLINE),trial-$(TRIAL_DEADLINE)-,)$(VERSION)-$(GOOS)-$(GOARCH)
DATE_FMT="+%FT%T%z"
# set SOURCE_DATE_EPOCH like this to use the last git commit timestamp
# export SOURCE_DATE_EPOCH=$(git log -1 --pretty=%ct) instead of the current time from running `date`
ifdef SOURCE_DATE_EPOCH
	BUILD_TIME ?= $(shell date -u -d "@$(SOURCE_DATE_EPOCH)" "$(DATE_FMT)" 2>/dev/null || date -u -r "$(SOURCE_DATE_EPOCH)" "$(DATE_FMT)" 2>/dev/null || date -u "$(DATE_FMT)")
else
	BUILD_TIME ?= $(shell date -u "$(DATE_FMT)")
endif
SHARD_WIDTH = 20
COMMIT := $(shell git describe --exact-match >/dev/null 2>&1 || git rev-parse --short HEAD)
LDFLAGS="-X github.com/featurebasedb/featurebase/v3.Version=$(VERSION) -X github.com/featurebasedb/featurebase/v3.BuildTime=$(BUILD_TIME) -X github.com/featurebasedb/featurebase/v3.Variant=$(VARIANT) -X github.com/featurebasedb/featurebase/v3.Commit=$(COMMIT) -X github.com/featurebasedb/featurebase/v3.TrialDeadline=$(TRIAL_DEADLINE)"
GO_VERSION=1.19
DOCKER_BUILD= # set to 1 to use `docker-build` instead of `build` when creating a release
BUILD_TAGS += 
TEST_TAGS = roaringparanoia
TEST_TIMEOUT=10m
RACE_TEST_TIMEOUT=10m
# size in GB to use for ramdisk, ?= so you can override it with env
# 4GB is not enough for `make test`, 8GB usually is.
RAMDISK_SIZE ?= 8

export GO111MODULE=on
export GOPRIVATE=github.com/molecula
export CGO_ENABLED=0
AWS_ACCOUNTID ?= undefined

# Run tests and compile Pilosa
default: test build

# Remove build directories
clean:
	rm -rf vendor build
	rm -f *.rpm *.deb

# Set up vendor directory using `go mod vendor`
vendor: go.mod
	$(GO) mod vendor

version:
	@echo $(VERSION)

# We build a list of packages that omits the IDK and batch packages because
# those packages require fancy environment setup.
GOPACKAGES := $(shell $(GO) list ./... | grep -v "/idk" | grep -v "/batch")

# Run test suite
test:
	$(GO) test $(GOPACKAGES) -tags='$(BUILD_TAGS) $(TEST_TAGS)' $(TESTFLAGS) -v -timeout $(TEST_TIMEOUT)

# Run test suite with race flag
test-race:
	CGO_ENABLED=1 $(GO) test $(GOPACKAGES) -tags='$(BUILD_TAGS) $(TEST_TAGS)' $(TESTFLAGS) -race -timeout $(RACE_TEST_TIMEOUT) -v

testv: testvsub

testv-race: testvsub-race

# testvsub: run go test -v in sub-directories in "local mode" with incremental output,
#            avoiding go -test ./... "package list mode" which doesn't give output
#            until the test run finishes. Package list mode makes it hard to
#            find which test is hung/deadlocked.
#
testvsub:
	@set -e; for pkg in $(GOPACKAGES); do \
			if [ $${pkg:0:38} == "github.com/featurebasedb/featurebase/v3/idk" ]; then \
				echo; echo "___ skipping subpkg $$pkg"; \
				continue; \
			fi; \
			echo; echo "___ testing subpkg $$pkg"; \
			$(GO) test -tags='$(BUILD_TAGS) $(TEST_TAGS)' $(TESTFLAGS) -v -timeout $(RACE_TEST_TIMEOUT) $$pkg || break; \
			echo; echo "999 done testing subpkg $$pkg"; \
		done

# make a $(RAMDISK_SIZE)GB RAMDisk. Speed up tests by running
# them with TMPDIR=/mnt/ramdisk.
ramdisk-linux:
	mount -o size=$(RAMDISK__SIZE)G -t tmpfs none /mnt/ramdisk

# make a $(RAMDISK_SIZE)GB RAMDisk. Speed up tests by running
# them with TMPDIR=/Volumes/RAMDisk. This is more important on
# OS X than it is on Linux, because there's performance issues
# with fsync on OS X that can make the SSD slow down to moving-platters
# drive speeds. Oops.
ramdisk-osx:
	diskutil erasevolume HFS+ 'RAMDisk' $$(hdiutil attach -nobrowse -nomount ram://$$(expr 2097152 \* $(RAMDISK_SIZE)))

detach-ramdisk-osx:
	hdiutil detach /Volumes/RAMDisk

testvsub-race:
	@set -e; for pkg in $(GOPACKAGES); do \
           echo; echo "___ testing subpkg $$pkg"; \
           CGO_ENABLED=1 $(GO) test -tags='$(BUILD_TAGS) $(TEST_TAGS)' $(TESTFLAGS) -v -race -timeout $(RACE_TEST_TIMEOUT) $$pkg || break; \
           echo; echo "999 done testing subpkg $$pkg"; \
        done


bench:
	$(GO) test $(GOPACKAGES) -bench=. -run=NoneZ -timeout=127m $(TESTFLAGS)

# Run test suite with coverage enabled
cover:
	mkdir -p build
	$(MAKE) test TESTFLAGS="-coverprofile=build/coverage.out"

# Run test suite with coverage enabled and view coverage results in browser
cover-viz: cover
	$(GO) tool cover -html=build/coverage.out

# Build featurebase
build:
	$(GO) build -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) ./cmd/featurebase

# Build fbsql
build-fbsql:
	$(GO) build -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) ./cmd/fbsql


package:
	GOOS=$(GOOS) GOARCH=$(GOARCH) $(MAKE) build
	GOOS=$(GOOS) GOARCH=$(GOARCH) $(MAKE) build-fbsql
	GOARCH=$(GOARCH) VERSION=$(VERSION) nfpm package --packager deb --target featurebase.$(VERSION).$(GOARCH).deb
	GOARCH=$(GOARCH) VERSION=$(VERSION) nfpm package --packager rpm --target featurebase.$(VERSION).$(GOARCH).rpm

# We allow setting a custom docker-compose "project". Multiple of the
# same docker-compose environment can exist simultaneously as long as
# they use different projects (the project name is prepended to
# container names and such). This is useful in a CI environment where
# we might be running multiple instances of the tests concurrently.
PROJECT ?= clustertests
DOCKER_COMPOSE = docker-compose -p $(PROJECT)

# Run cluster integration tests using docker. Requires docker daemon to be
# running and docker-compose to be installed.
clustertests: vendor
	$(DOCKER_COMPOSE) -f internal/clustertests/docker-compose.yml down
	$(DOCKER_COMPOSE) -f internal/clustertests/docker-compose.yml build
	$(DOCKER_COMPOSE) -f internal/clustertests/docker-compose.yml up -d pilosa1 pilosa2 pilosa3
	PROJECT=$(PROJECT) $(DOCKER_COMPOSE) -f internal/clustertests/docker-compose.yml run client1
	$(DOCKER_COMPOSE) -f internal/clustertests/docker-compose.yml down

# Run the cluster tests with authentication enabled
AUTH_ARGS="-c /go/src/github.com/featurebasedb/featurebase/internal/clustertests/testdata/featurebase.conf"
authclustertests: vendor
	CLUSTERTESTS_FB_ARGS=$(AUTH_ARGS) $(DOCKER_COMPOSE) -f internal/clustertests/docker-compose.yml down
	CLUSTERTESTS_FB_ARGS=$(AUTH_ARGS) $(DOCKER_COMPOSE) -f internal/clustertests/docker-compose.yml build
	CLUSTERTESTS_FB_ARGS=$(AUTH_ARGS) $(DOCKER_COMPOSE) -f internal/clustertests/docker-compose.yml up -d pilosa1 pilosa2 pilosa3
	PROJECT=$(PROJECT) ENABLE_AUTH=1 $(DOCKER_COMPOSE) -f internal/clustertests/docker-compose.yml run client1
	CLUSTERTESTS_FB_ARGS=$(AUTH_ARGS) $(DOCKER_COMPOSE) -f internal/clustertests/docker-compose.yml down

# Install FeatureBase and IDK
install: install-featurebase install-idk install-fbsql

install-featurebase:
	$(GO) install -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) ./cmd/featurebase

install-idk:
	$(MAKE) -C ./idk install

install-fbsql:
	$(GO) install ./cmd/fbsql

# Build the lattice assets
build-lattice:
	docker build -t lattice:build ./lattice
	export LATTICE=`docker create lattice:build`; docker cp $$LATTICE:/lattice/. ./lattice/build && docker rm $$LATTICE

# `go generate` protocol buffers
generate-protoc: require-protoc require-protoc-gen-gofast
	$(GO) generate github.com/featurebasedb/featurebase/v3/pb

# `go generate` statik assets (lattice UI)
generate-statik: build-lattice require-statik
	$(GO) generate github.com/featurebasedb/featurebase/v3/statik

# `go generate` statik assets (lattice UI) in Docker
generate-statik-docker: build-lattice
	docker run --rm -t -v $(PWD):/pilosa golang:1.15.8 sh -c "go get github.com/rakyll/statik && /go/bin/statik -src=/pilosa/lattice/build -dest=/pilosa -f"

# `go generate` stringers
generate-stringer:
	$(GO) generate github.com/featurebasedb/featurebase/v3

generate-pql: require-peg
	cd pql && peg -inline pql.peg && cd ..

generate-proto-grpc: require-protoc require-protoc-gen-go
	protoc -I proto proto/pilosa.proto --go_out=plugins=grpc:proto
#	address re-generation here only if we need to	
#	protoc -I proto proto/vdsm.proto --go_out=plugins=grpc:proto

# `go generate` all needed packages
generate: generate-protoc generate-statik generate-stringer generate-pql

# Create release using Docker
docker-release:
	$(MAKE) docker-build GOOS=linux GOARCH=amd64
	$(MAKE) docker-build GOOS=linux GOARCH=arm64
	$(MAKE) docker-build GOOS=darwin GOARCH=amd64
	$(MAKE) docker-build GOOS=darwin GOARCH=arm64

# Build a release in Docker
docker-build: vendor
	docker build \
	    --build-arg GO_VERSION=$(GO_VERSION) \
	    --build-arg MAKE_FLAGS="TRIAL_DEADLINE=$(TRIAL_DEADLINE) GOOS=$(GOOS) GOARCH=$(GOARCH)" \
	    --build-arg SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH) \
	    --target pilosa-builder \
	    --tag featurebase:build .
	docker create --name featurebase-build featurebase:build
	mkdir -p build/featurebase-$(VERSION_ID)
	docker cp featurebase-build:/pilosa/build/. ./build/featurebase-$(VERSION_ID)
	cp NOTICE install/featurebase.conf install/featurebase*.service ./build/featurebase-$(VERSION_ID)
	docker rm featurebase-build
	tar -cvz -C build -f build/featurebase-$(VERSION_ID).tar.gz featurebase-$(VERSION_ID)/

# Create Docker image from Dockerfile
docker-image: vendor
	docker build \
	    --build-arg GO_VERSION=$(GO_VERSION) \
	    --build-arg MAKE_FLAGS="TRIAL_DEADLINE=$(TRIAL_DEADLINE)" \
	    --tag featurebase:$(VERSION) .
	@echo Created docker image: featurebase:$(VERSION)

docker-image-featurebase: vendor
	docker build \
	    --build-arg GO_VERSION=$(GO_VERSION) \
		--file Dockerfile-dax \
		--tag dax/featurebase .

docker-image-featurebase-test: vendor
	docker build \
	    --build-arg GO_VERSION=$(GO_VERSION) \
		--file Dockerfile-clustertests \
		--tag dax/featurebase-test .


# build-for-quick builds a linux featurebase binary outside of docker
# (which is much faster for some reason), and places it in the .quick
# subdirectory.
build-for-quick:
	GOOS=linux $(MAKE) build FLAGS="-o .quick/fb_linux"

# docker-image-featurebase-quick uses a pre-built featurebase binary
# to quickly create a fresh docker image without needing to send the
# context of the featurebase top level directory.
docker-image-featurebase-quick: build-for-quick
	docker build \
	    --build-arg GO_VERSION=$(GO_VERSION) \
	    --file Dockerfile-dax-quick \
	    --tag dax/featurebase ./.quick/


docker-image-datagen: vendor
	docker build --tag dax/datagen --file Dockerfile-datagen .

ecr-push-featurebase: docker-login
	docker tag dax/featurebase:latest $(AWS_ACCOUNTID).dkr.ecr.us-east-2.amazonaws.com/dax/featurebase:latest
	docker push $(AWS_ACCOUNTID).dkr.ecr.us-east-2.amazonaws.com/dax/featurebase:latest

ecr-push-datagen: docker-login
	docker tag dax/datagen:latest $(AWS_ACCOUNTID).dkr.ecr.us-east-2.amazonaws.com/dax/datagen:latest
	docker push $(AWS_ACCOUNTID).dkr.ecr.us-east-2.amazonaws.com/dax/datagen:latest

docker-login:
	aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin $(AWS_ACCOUNTID).dkr.ecr.us-east-2.amazonaws.com

# Create docker image (alias)
docker: docker-image # alias

# Tag and push a Docker image
docker-tag-push: vendor
	docker tag "featurebase:$(VERSION)" $(DOCKER_TARGET)
	docker push $(DOCKER_TARGET)
	@echo Pushed docker image: $(DOCKER_TARGET)

# These commands (docker-idk and docker-idk-tag-push)
# are designed to be used in CI.
# docker-idk builds idk docker images and tags them - intended for use in CI.
docker-idk: vendor
	docker build \
		-f idk/Dockerfile \
		--build-arg GO_VERSION=$(GO_VERSION) \
		--build-arg MAKE_FLAGS="GOOS=$(GOOS) GOARCH=$(GOARCH) BUILD_CGO=$(BUILD_CGO)" \
		--tag registry.gitlab.com/molecula/featurebase/idk:$(VERSION_ID) .
	@echo Created docker image: registry.gitlab.com/molecula/featurebase/idk:$(VERSION_ID)
# docker-idk-tag-push pushes tagged docker images to the GitLab container
# registry - intended for use in CI.
docker-idk-tag-push:
	docker push registry.gitlab.com/molecula/featurebase/idk:$(VERSION_ID)
	@echo Pushed docker image: registry.gitlab.com/molecula/featurebase/idk:$(VERSION_ID)

# Run golangci-lint
golangci-lint: require-golangci-lint
	golangci-lint run --timeout 3m --skip-files '.*\.peg\.go'

# Alias
linter: golangci-lint

# Better alias
ocd: golangci-lint

######################
# Build dependencies #
######################

# Verifies that needed build dependency is installed. Errors out if not installed.
require-%:
	$(if $(shell command -v $* 2>/dev/null),\
		$(info Verified build dependency "$*" is installed.),\
		$(error Build dependency "$*" not installed. To install, try `make install-$*`))

install-build-deps: install-protoc-gen-gofast install-protoc install-statik install-peg

install-statik:
	go install github.com/rakyll/statik@latest

install-protoc-gen-gofast:
	GO111MODULE=off $(GO) get -u github.com/gogo/protobuf/protoc-gen-gofast

install-protoc:
	@echo This tool cannot automatically install protoc. Please download and install protoc from https://google.github.io/proto-lens/installing-protoc.html
	@echo On mac, brew install protobuf seems to work.
	@echo As of the commit that added this line, protoc-gen-gofast was at 226206f39bd7, and the protoc version in use was:
	@echo $$ protoc --version
	@echo libprotoc 3.19.4

install-peg:
	GO111MODULE=off $(GO) get github.com/pointlander/peg

install-golangci-lint:
	GO111MODULE=off $(GO) get github.com/golangci/golangci-lint/cmd/golangci-lint

test-external-lookup:
	$(GO) test . -tags='$(BUILD_TAGS) $(TEST_TAGS)' $(TESTFLAGS) -run ^TestExternalLookup$$ -externalLookupDSN $(EXTERNAL_LOOKUP_DSN)

bnf:
	ebnf2railroad --no-overview-diagram  --no-optimizations ./sql3/sql3.ebnf
