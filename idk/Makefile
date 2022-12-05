GO ?= go
GOOS ?= $(shell $(GO) env GOOS)
GOARCH ?= $(shell $(GO) env GOARCH)
GOPATH ?= $(shell $(GO) env GOPATH)
GO_VERSION=1.19
GO_BUILD_FLAGS=
ODBC_ENABLED=
BINOUT ?= bin
VERSION := $(shell git describe --tags 2> /dev/null || git rev-parse --verify --short=7 HEAD)
VERSION_ID = $(VERSION)-$(GOOS)-$(GOARCH)
DATE_FMT="+%FT%T%z"
# set SOURCE_DATE_EPOCH like this to use the last git commit timestamp
# export SOURCE_DATE_EPOCH=$(git log -1 --pretty=%ct) instead of the current time from running `date`.
ifdef SOURCE_DATE_EPOCH
	BUILD_TIME ?= $(shell date -u -d "@$(SOURCE_DATE_EPOCH)" "$(DATE_FMT)" 2>/dev/null || date -u -r "$(SOURCE_DATE_EPOCH)" "$(DATE_FMT)" 2>/dev/null || date -u "$(DATE_FMT)")
else
	BUILD_TIME ?= $(shell date -u "$(DATE_FMT)")
endif
AWS_ACCOUNTID ?= undefined
BRANCH_NAME ?= ""

# We allow setting a custom docker-compose "project". Multiple of the
# same docker-compose environment can exist simultaneously as long as
# they use different projects (the project name is prepended to
# container names and such). This is useful in a CI environment where
# we might be running multiple instances of the tests concurrently.
PROJECT ?= idk
DOCKER_COMPOSE = docker-compose -p $(PROJECT)

UNAME_P := $(shell uname -p)
ifneq ($(filter arm%,$(UNAME_P)),)
	IS_ARM := 1
else
	IS_ARM := 0
endif

ifneq ($(filter Darwin%,$(shell uname)),)
	IS_MAC := 1
else
	IS_MAC := 0
endif

LDFLAGS="-X 'github.com/featurebasedb/featurebase/v3/idk.Version=$(VERSION)' -X 'github.com/featurebasedb/featurebase/v3/idk.BuildTime=$(BUILD_TIME)'"
LDFLAGS_STATIC="-linkmode external -extldflags \"-static\" -X 'github.com/featurebasedb/featurebase/v3/idk.Version=$(VERSION)' -X 'github.com/featurebasedb/featurebase/v3/idk.BuildTime=$(BUILD_TIME)' "
export GOPRIVATE=github.com/molecula
export CGO_ENABLED=1

# Cloud  Deployment environment (dev, prod, staging, etc.), specified with the value of MCLOUD_ENV
# E.g. `MCLOUD_ENV=test make docker-push-ecr`
MCLOUD_ENV ?= dev
MCLOUD_ENV_FILE=.cloud-env.$(MCLOUD_ENV)
-include $(MCLOUD_ENV_FILE)

PROFILE ?=
AWS_REGION ?=
ECR_URL ?=
ECR_REPO ?= idk


BINS := $(patsubst cmd/%,$(BINOUT)/%,$(wildcard cmd/*))
.PHONY: install build release-build release test $(BINS)

install:
ifeq ($(filter $(IS_ARM),$(IS_MAC)), 1)
	export HOMEBREW_NO_AUTO_UPDATE=1 && [ "${PKG_CONFIG_PATH}" != "$$(brew --prefix openssl)/lib/pkgconfig" ] && (brew list pkg-config || brew install pkg-config) && (brew list librdkafka || brew install librdkafka) && (brew list openssl || brew install openssl) && export PKG_CONFIG_PATH="$$(brew --prefix openssl)/lib/pkgconfig"; \
	$(GO) install -ldflags $(LDFLAGS) $(GO_BUILD_FLAGS) -tags dynamic ./cmd/...
else
	$(GO) install -ldflags $(LDFLAGS) $(GO_BUILD_FLAGS) ./cmd/...
endif

$(BINS):
	$(GO) build -ldflags $(LDFLAGS) $(GO_BUILD_FLAGS) -o $@ ./cmd/$(notdir $@)

$(BINOUT)/molecula-consumer-sql-odbc:
	CGO_ENABLED=1 $(GO) build -tags=odbc -ldflags $(LDFLAGS) $(GO_BUILD_FLAGS) -o $@ ./cmd/molecula-consumer-sql

build_non_cgo: 
	CGO_ENABLED=0 $(GO) build -ldflags $(LDFLAGS) $(GO_BUILD_FLAGS) -o bin/ingester ./cmd/ingester
	CGO_ENABLED=0 $(GO) build -ldflags $(LDFLAGS) $(GO_BUILD_FLAGS) -o bin/molecula-consumer-csv ./cmd/molecula-consumer-csv
	CGO_ENABLED=0 $(GO) build -ldflags $(LDFLAGS) $(GO_BUILD_FLAGS) -o bin/molecula-consumer-kafka-static ./cmd/molecula-consumer-kafka-static
	CGO_ENABLED=0 $(GO) build -ldflags $(LDFLAGS) $(GO_BUILD_FLAGS) -o bin/molecula-consumer-sql ./cmd/molecula-consumer-sql
	CGO_ENABLED=0 $(GO) build -ldflags $(LDFLAGS) $(GO_BUILD_FLAGS) -o bin/molecula-consumer-github ./cmd/molecula-consumer-github
	CGO_ENABLED=0 $(GO) build -ldflags $(LDFLAGS) $(GO_BUILD_FLAGS) -o bin/molecula-consumer-kinesis ./cmd/molecula-consumer-kinesis

build_cgo: 
ifeq ($(GOARCH), arm64)
	CGO_ENABLED=1 $(GO) build -tags dynamic $(GO_BUILD_FLAGS) -o bin/molecula-consumer-kafka ./cmd/molecula-consumer-kafka
	CGO_ENABLED=1 $(GO) build -tags dynamic $(GO_BUILD_FLAGS) -o bin/molecula-consumer-kafka-delete ./cmd/molecula-consumer-kafka-delete
	CGO_ENABLED=1 $(GO) build -tags dynamic $(GO_BUILD_FLAGS) -o bin/kafkagen ./cmd/kafkagen
	CGO_ENABLED=1 $(GO) build -tags dynamic $(GO_BUILD_FLAGS) -o bin/kafkaput ./cmd/kafkaput
	CGO_ENABLED=1 $(GO) build -tags dynamic $(GO_BUILD_FLAGS) -o bin/datagen ./cmd/datagen
	CGO_ENABLED=1 $(GO) build -tags dynamic $(GO_BUILD_FLAGS) -o bin/bankgen ./cmd/bankgen
	CGO_ENABLED=1 $(GO) build -tags dynamic $(GO_BUILD_FLAGS) -o bin/molecula-consumer-kafka-sasl ./cmd/molecula-consumer-kafka-sasl
endif
ifeq ($(GOARCH), amd64)
	CC=/usr/bin/musl-gcc CGO_ENABLED=1 $(GO) build -tags "musl static" -ldflags $(LDFLAGS_STATIC) $(GO_BUILD_FLAGS) -o bin/molecula-consumer-kafka ./cmd/molecula-consumer-kafka
	CC=/usr/bin/musl-gcc CGO_ENABLED=1 $(GO) build -tags "musl static" -ldflags $(LDFLAGS_STATIC) $(GO_BUILD_FLAGS) -o bin/molecula-consumer-kafka-delete ./cmd/molecula-consumer-kafka-delete
	CC=/usr/bin/musl-gcc CGO_ENABLED=1 $(GO) build -tags "musl static" -ldflags $(LDFLAGS_STATIC) $(GO_BUILD_FLAGS) -o bin/kafkagen ./cmd/kafkagen
	CC=/usr/bin/musl-gcc CGO_ENABLED=1 $(GO) build -tags "musl static" -ldflags $(LDFLAGS_STATIC) $(GO_BUILD_FLAGS) -o bin/kafkaput ./cmd/kafkaput
	CC=/usr/bin/musl-gcc CGO_ENABLED=1 $(GO) build -tags "musl static" -ldflags $(LDFLAGS_STATIC) $(GO_BUILD_FLAGS) -o bin/datagen ./cmd/datagen
	CC=/usr/bin/musl-gcc CGO_ENABLED=1 $(GO) build -tags "musl static" -ldflags $(LDFLAGS_STATIC) $(GO_BUILD_FLAGS) -o bin/bankgen ./cmd/bankgen
	CC=/usr/bin/musl-gcc CGO_ENABLED=1 $(GO) build -tags "musl static" -ldflags $(LDFLAGS_STATIC) $(GO_BUILD_FLAGS) -o bin/molecula-consumer-kafka-sasl ./cmd/molecula-consumer-kafka-sasl
endif

	CGO_ENABLED=1 $(GO) build -tags=odbc -ldflags $(LDFLAGS) $(GO_BUILD_FLAGS) -o bin/molecula-consumer-sql-odbc ./cmd/molecula-consumer-sql

BUILD_CGO ?= 0
build:
	@echo GOOS=$(GOOS) GOARCH=$(GOARCH) uname -p=$(UNAME_P) build_cgo=$(BUILD_CGO)
	make build_non_cgo
ifeq ($(BUILD_CGO), 1)
	make build_cgo
endif

release-build:
	mkdir -p build/molecula-consumer-$(VERSION_ID)/
	$(MAKE) build BINOUT=build/molecula-consumer-$(VERSION_ID)/
	tar -cvz -C build -f build/molecula-consumer-$(VERSION_ID).tar.gz molecula-consumer-$(VERSION_ID)/
	@echo Created release build: build/molecula-consumer-$(VERSION_ID).tar.gz

release: check-clean
	make release-build GOOS=linux GOARCH=amd64
	make release-build GOOS=linux GOARCH=arm64
	make release-build GOOS=darwin GOARCH=amd64
	make release-build GOOS=darwin GOARCH=arm64

test:
	$(GO) test ./...

test-local: testenv
	IDK_TEST_CERT_PATH=$(shell pwd)/testenv/certs \
	IDK_TEST_KAFKA_HOST=localhost:9092 \
	IDK_TEST_PILOSA_GRPC_HOST=localhost:20101 \
	IDK_TEST_PILOSA_HOST=localhost:10101 \
	IDK_TEST_PILOSA_TLS_HOST=https://localhost:10111 \
	IDK_TEST_REGISTRY_HOST=localhost:8081 \
	$(GO) test $(TESTFLAGS) ./...

vendor: ../go.mod
	$(GO) mod vendor

build-%:
	$(DOCKER_COMPOSE) build $*

pull-%:
	$(DOCKER_COMPOSE) pull $*

save-%-logs:
	$(DOCKER_COMPOSE) logs $* > ./testdata/$(PROJECT)_$*_logs.txt


start-all: testenv build-wait
	echo "branch name" ${BRANCH_NAME}
	$(DOCKER_COMPOSE) up -d zookeeper
	$(DOCKER_COMPOSE) run -T  wait zookeeper 'echo "ruok" | nc -w 2 zookeeper 2181 | grep imok'
	$(DOCKER_COMPOSE) up -d dax
	$(DOCKER_COMPOSE) up -d kafka
	$(DOCKER_COMPOSE) up -d schema-registry
	$(DOCKER_COMPOSE) up -d postgres
	$(DOCKER_COMPOSE) run -T  wait postgres pg_isready -h postgres -p 5432 -U postgres
	BRANCH_NAME=${BRANCH_NAME} $(DOCKER_COMPOSE) up -d pilosa
	BRANCH_NAME=${BRANCH_NAME} $(DOCKER_COMPOSE) up -d pilosa-tls
	BRANCH_NAME=${BRANCH_NAME} $(DOCKER_COMPOSE) up -d pilosa-auth
	$(DOCKER_COMPOSE) run -T  wait pilosa curl --silent --fail http://pilosa:10101/status
	$(DOCKER_COMPOSE) run -T  wait pilosa-tls curl --silent --cacert /certs/ca.crt --key /certs/theclient.key --cert /certs/theclient.crt --fail https://pilosa-tls:10111/status
	$(DOCKER_COMPOSE) run -T  wait pilosa-auth curl --silent --fail http://pilosa-auth:10105/version
	$(DOCKER_COMPOSE) run -T wait dax curl --silent --fail http://dax:8080/computer0/status
	$(DOCKER_COMPOSE) run -T  wait kafka nc -z kafka 9092
	$(DOCKER_COMPOSE) run -T  wait schema-registry curl --silent --fail http://schema-registry:8081/config

start-postgres: build-wait testenv
	$(DOCKER_COMPOSE) up -d postgres
	$(DOCKER_COMPOSE) run -T  wait postgres pg_isready -h postgres -p 5432 -U postgres

start-pilosa: build-pilosa start-postgres build-wait testenv
	$(DOCKER_COMPOSE) up -d pilosa
	$(DOCKER_COMPOSE) run -T  wait pilosa curl --silent --fail http://pilosa:10101/status

start-pilosa-tls: build-pilosa-tls build-wait testenv
	$(DOCKER_COMPOSE) up -d pilosa-tls
	$(DOCKER_COMPOSE) run -T  wait pilosa-tls curl --silent --cacert /certs/ca.crt --key /certs/theclient.key --cert /certs/theclient.crt --fail https://pilosa-tls:10111/status

start-pilosa-auth: build-pilosa-auth build-wait testenv
	$(DOCKER_COMPOSE) up -d pilosa-auth
	$(DOCKER_COMPOSE) run -T  wait pilosa-auth curl --silent --fail http://pilosa-auth:10105/version

start-zookeeper: pull-zookeeper testenv
	$(DOCKER_COMPOSE) up -d zookeeper
	$(DOCKER_COMPOSE) run -T  wait zookeeper 'echo "ruok" | nc -w 2 zookeeper 2181 | grep imok'

start-kafka: pull-kafka start-zookeeper build-wait testenv
	$(DOCKER_COMPOSE) up -d kafka
	$(DOCKER_COMPOSE) run -T  wait kafka nc -z kafka 9092

start-schema-registry: start-zookeeper start-kafka pull-schema-registry build-wait testenv
	$(DOCKER_COMPOSE) up -d schema-registry
	$(DOCKER_COMPOSE) run -T  wait schema-registry curl --silent --fail http://schema-registry:8081/config

startup-old: start-postgres start-pilosa start-pilosa-tls start-pilosa-auth start-zookeeper start-kafka start-schema-registry
startup: start-all

shutdown:
	$(DOCKER_COMPOSE) down -v --remove-orphans

test-all: testenv
	$(MAKE) startup
	$(MAKE) test-run
	$(DOCKER_COMPOSE) logs kafka | grep "Kafka version"
	$(MAKE) shutdown


test-all-race: testenv
	$(MAKE) startup
	$(MAKE) test-run-race
	$(MAKE) shutdown

test-all-kafka-sasl: testenv
	$(MAKE) startup
	$(MAKE) test-run-kafka-sasl
	$(MAKE) shutdown

TCMD ?= ./...
# do "make startup", then e.g. "make test-run-local TCMD='-run=MyFavTest ./kafka'"
test-run-local: vendor
	pwd
	$(DOCKER_COMPOSE) build idk-test
	$(DOCKER_COMPOSE) run -T idk-test go test -mod=vendor -tags=odbc,dynamic $(TCMD)


TPKG ?= ./...
test-run: testenv vendor
	$(DOCKER_COMPOSE) build idk-test
	$(DOCKER_COMPOSE) run -T idk-test bash -c "set -o pipefail; go test -v -mod=vendor -tags=odbc,dynamic $(TPKG) -covermode=atomic -coverpkg=$(TPKG) -coverprofile=/testdata/$(PROJECT)_base_coverage.out"
	$(DOCKER_COMPOSE) run -T idk-test /go/src/github.com/molecula/featurebase/idk/reingest_test.sh


test-run-race: testenv vendor
	$(DOCKER_COMPOSE) build idk-test
	$(DOCKER_COMPOSE) run -T idk-test bash -c "set -o pipefail; go test -v -mod=vendor -race -covermode=atomic -tags=dynamic $(TPKG) -coverpkg=$(TPKG) -timeout=30m -coverprofile=/testdata/$(PROJECT)_race_coverage.out"

test-run-kafka-sasl: testenv vendor
	$(DOCKER_COMPOSE) build idk-test
	$(DOCKER_COMPOSE) run -T idk-test bash -c "set -o pipefail; go test -v --tags=kafka_sasl -mod=vendor -race -timeout=30m $(TPKG) -covermode=atomic -coverpkg=$(TPKG) -coverprofile=/testdata/$(PROJECT)_sasl_coverage.out"


testenv: testenv/certs

testenv/certs:
	certstrap --depot-path testenv/certs init --common-name ca --passphrase "" --expires "100 years"
	certstrap --depot-path=testenv/certs request-cert --domain localhost --common-name localhost --passphrase ""
	certstrap --depot-path=testenv/certs sign --CA ca --expires "100 years" localhost
	certstrap --depot-path=testenv/certs request-cert --domain pilosa-tls --common-name pilosa-tls --passphrase ""
	certstrap --depot-path=testenv/certs sign --CA ca --expires "100 years" pilosa-tls
	certstrap --depot-path=testenv/certs request-cert --domain theclient --common-name theclient --passphrase ""
	certstrap --depot-path=testenv/certs sign --CA ca --expires "100 years" theclient

# Create release using Docker
docker-release:
	@if [ $(IS_ARM) = 1 ]; then \
		echo Building linux-amd64 image on $(UNAME_P) is not supported; \
	else \
		$(MAKE) docker-build GOOS=linux GOARCH=amd64; \
	fi
	$(MAKE) docker-build GOOS=darwin GOARCH=amd64
	$(MAKE) docker-build GOOS=darwin GOARCH=arm64
	$(MAKE) docker-build GOOS=linux GOARCH=arm64


# This allows multiple concurrent builds to happen in CI without
# creating container name conflicts and such. (different BUILD_NAMEs
# are passed in from gitlab-ci.yml)
BUILD_NAME ?= idk-build

# Build a release in Docker
docker-build: vendor
	DOCKER_BUILDKIT=0 docker build \
		-f ../idk/Dockerfile \
	    --build-arg GO_VERSION=$(GO_VERSION) \
	    --build-arg MAKE_FLAGS="GOOS=$(GOOS) GOARCH=$(GOARCH) BUILD_CGO=$(BUILD_CGO)" \
	    --build-arg GO_BUILD_FLAGS=$(GO_BUILD_FLAGS) \
            --build-arg SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH) \
	    --target builder \
	    --tag idk:$(BUILD_NAME) ../.
	mkdir -p build/idk-$(GOOS)-$(GOARCH)
	docker create --name $(BUILD_NAME) idk:$(BUILD_NAME)
	docker cp $(BUILD_NAME):/featurebase/idk/bin/. ./build/idk-$(GOOS)-$(GOARCH)
	docker rm $(BUILD_NAME)

# Create Docker image from Dockerfile
docker-image: vendor
	docker build \
	    --build-arg GO_VERSION=$(GO_VERSION) \
	    --build-arg MAKE_FLAGS="GOOS=$(GOOS) GOARCH=$(GOARCH) BUILD_CGO=$(BUILD_CGO)" \
	    --tag registry.gitlab.com/molecula/featurebase/idk:$(VERSION_ID) .
	@echo Created docker image: registry.gitlab.com/molecula/featurebase/idk:$(VERSION_ID)

docker: docker-image

docker-tag-push:
	docker push registry.gitlab.com/molecula/featurebase/idk:$(VERSION_ID)
	@echo Pushed docker image: registry.gitlab.com/molecula/featurebase/idk:$(VERSION_ID)

build-datagen:
	CGO_ENABLED=1 $(GO) build -tags dynamic $(GO_BUILD_FLAGS) -o build/datagen ./cmd/datagen

clean:
	$(MAKE) shutdown
	rm -rf testenv

# Error out if there are untracked changes in Git
check-clean:
ifndef SKIP_CHECK_CLEAN
	$(if $(shell git status --porcelain),$(error Git status is not clean! Please commit or checkout/reset changes.))
endif

docker-push-ecr: docker-image aws-login
	docker tag idk:$(VERSION) $(ECR_URL)/$(ECR_REPO):$(VERSION)
	aws ecr get-login-password --region $(AWS_REGION) --profile $(PROFILE)| docker login --username AWS --password-stdin $(ECR_URL)
	docker push $(ECR_URL)/$(ECR_REPO):$(VERSION)

aws-login:
	aws sso login --profile $(PROFILE)


# Build mock implementations of AWS service API interfaces for unit testing.
#
# These are run-once/rarely targets as they produce code artifacts only used
# during `go test`. An automated CI/CD process does NOT need to invoke every time.
MOCKS = S3 Kinesis SQS
install-mock-generator:
	@which $(GOPATH)/bin/mockery || (echo "Installing missing dependency 'mockery' to generate mock implementations for unit testing." && \
                                         $(GO) install 'github.com/vektra/mockery/v2@latest')

update-mocks: $(addprefix update-mocks-, $(MOCKS))

update-mocks-%: install-mock-generator
	$(eval AWS_SERVICE := $(shell echo $* | tr '[:upper:]' '[:lower:]'))
	$(eval AWS_SDK_VERSION := $(shell grep 'github.com/aws/aws-sdk-go' ../go.mod | cut -d ' ' -f 2))
	echo Generating mock for AWS service $(AWS_SERVICE) and SDK version $(AWS_SDK_VERSION) && \
	$(GOPATH)/bin/mockery --name $*API --output idktest/mocks --filename $(AWS_SERVICE).go --dir $(GOPATH)/pkg/mod/github.com/aws/aws-sdk-go@$(AWS_SDK_VERSION)/service/$(AWS_SERVICE)/$(AWS_SERVICE)iface
