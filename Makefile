default:

# Temporary workaround to avoid testing vendor directory.
test:
	go test $(go list ./... | grep -v /vendor/)
