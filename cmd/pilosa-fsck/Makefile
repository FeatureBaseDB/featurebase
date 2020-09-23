.PHONY: install build release

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
GOOS = $(shell go env GOOS)

# Install pilosa-fsck
install:
	go install -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) 

# Compile pilosa-fsck
build:
	go build -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS)

REL = release-pilosa-fsck.$(COMMIT).$(GOOS)

release:
	mkdir $(REL)
	cd release-pilosa-fsck; tar cf - . |(cd ../$(REL); tar xf - )
	go build -tags='$(BUILD_TAGS)' -ldflags $(LDFLAGS) $(FLAGS) -o $(REL)/pilosa-fsck
	tar cf - $(REL) | gzip > $(REL).tar.gz
	rm -rf $(REL)
	mv $(REL).tar.gz ../..

clean:
	find . -name pilosa-fsck | xargs rm -f
	rm -f release-pilosa-fsck*.tar.gz
