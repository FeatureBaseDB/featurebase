.PHONY: build-runners push-runners

TAG_ROOT=ghcr.io/featurebasedb/examples
TAG_FB_RUNNER=$(TAG_ROOT)/featurebase-runner
TAG_INGEST_RUNNER=$(TAG_ROOT)/ingest-runner
TAG_VERSION=latest

build-runners:
	docker build --target=featurebase -t$(TAG_FB_RUNNER):$(TAG_VERSION) -f Dockerfile ../
	docker build --target=ingest -t$(TAG_INGEST_RUNNER):$(TAG_VERSION) -f Dockerfile ../

push-runners:
	docker push $(TAG_FB_RUNNER):$(TAG_VERSION)
	docker push $(TAG_INGEST_RUNNER):$(TAG_VERSION)
