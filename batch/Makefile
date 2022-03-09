GO ?= go

# We allow setting a custom docker-compose "project". Multiple of the
# same docker-compose environment can exist simultaneously as long as
# they use different projects (the project name is prepended to
# container names and such). This is useful in a CI environment where
# we might be running multiple instances of the tests concurrently.
PROJECT ?= batch
DOCKER_COMPOSE = docker-compose -p $(PROJECT)

vendor: ../go.mod
	$(GO) mod vendor

build-%:
	$(DOCKER_COMPOSE) build $*

test-all:
	$(MAKE) startup
	$(MAKE) test-run
	$(MAKE) shutdown

start-all: build-wait
	$(DOCKER_COMPOSE) up -d featurebase
	$(DOCKER_COMPOSE) run -T wait featurebase curl --silent --fail http://featurebase:10101/status

startup: start-all

shutdown:
	$(DOCKER_COMPOSE) down -v --remove-orphans

save-%-logs:
	$(DOCKER_COMPOSE) logs $* > ./testdata/$(PROJECT)_$*_logs.txt

TCMD ?= ./...
# do "make startup", then e.g. "make test-run-local TCMD='-run=MyFavTest ./kafka'"
test-run-local:
	pwd
	$(DOCKER_COMPOSE) build batch-test
	$(DOCKER_COMPOSE) run -T batch-test go test -mod=vendor -tags=odbc,dynamic $(TCMD)

TPKG ?= ../...
test-run: vendor
	$(DOCKER_COMPOSE) build batch-test
	$(DOCKER_COMPOSE) run -T batch-test bash -c "set -o pipefail; go test -v -mod=vendor -tags=odbc,dynamic ./... -covermode=atomic -coverpkg=$(TPKG) -json -coverprofile=/testdata/$(PROJECT)_base_coverage.out  | tee /testdata/$(PROJECT)_report.out"
