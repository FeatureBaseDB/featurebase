.PHONY: glide vendor-update docker pilosa crossbuild install generate statik release test cover cover-pkg cover-viz

GLIDE := $(shell command -v glide 2>/dev/null)
STATIK := $(shell command -v statik 2>/dev/null)
PROTOC := $(shell command -v protoc 2>/dev/null)
VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
IDENTIFIER := $(VERSION)-$(GOOS)-$(GOARCH)
CLONE_URL=github.com/pilosa/pilosa
PKGS := $(shell cd $(GOPATH)/src/$(CLONE_URL); go list ./... | grep -v vendor)
BUILD_TIME=`date -u +%FT%T%z`
LDFLAGS="-X github.com/pilosa/pilosa.Version=$(VERSION) -X github.com/pilosa/pilosa.BuildTime=$(BUILD_TIME)"

default: test pilosa

$(GOPATH)/bin:
	mkdir $(GOPATH)/bin

glide: $(GOPATH)/bin
ifndef GLIDE
	curl https://glide.sh/get | sh
endif

$(GLIDE):
	make glide

vendor: $(GLIDE) glide.yaml
ifndef GLIDE
	curl https://glide.sh/get | sh
endif
	glide install

glide.lock: glide glide.yaml
	glide update

vendor-update: glide.lock

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

crossbuild: vendor
	mkdir -p build/pilosa-$(IDENTIFIER)
	make pilosa FLAGS="-o build/pilosa-$(IDENTIFIER)/pilosa"
	cp LICENSE README.md build/pilosa-$(IDENTIFIER)
	tar -cvz -C build -f build/pilosa-$(IDENTIFIER).tar.gz pilosa-$(IDENTIFIER)/
	@echo "Created release build: build/pilosa-$(IDENTIFIER).tar.gz"

release:
	make crossbuild GOOS=linux GOARCH=amd64
	make crossbuild GOOS=linux GOARCH=386
	make crossbuild GOOS=darwin GOARCH=amd64

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
