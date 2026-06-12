.PHONY: test
test:
	go test -tags unit ./...

.PHONY: test-race
test-race:
	CGO_ENABLED=1 go test -race -tags unit ./...

.PHONY: test-integration
test-integration:
	go test -tags integration -v -count=1 -timeout 15m ./tests/integration/...

.PHONY: lint
lint:
	golangci-lint run
