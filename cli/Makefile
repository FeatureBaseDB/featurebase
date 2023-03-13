.PHONY: test testv test-integration testv-integration

GO=go

test:
	$(GO) test ./... -short

testv:
	$(GO) test -v ./... -short

test-integration:
	$(GO) test . -count 1 -timeout 20m -run TestCLIIntegration/$(RUN)

testv-integration:
	$(GO) test -v . -count 1 -timeout 20m -run TestCLIIntegration/$(RUN)
