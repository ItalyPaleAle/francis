.PHONY: test
test:
	go test -tags unit ./...

.PHONY: test-race
test-race:
	CGO_ENABLED=1 go test -race -tags unit ./...

.PHONY: test-integration
test-integration:
	go test -tags integration -count=1 -timeout 15m ./tests/integration/...

.PHONY: lint
lint:
	golangci-lint run

# Regenerate the mocks in internal/mocks from the interfaces listed in .mockery.yml
# Run this after changing any mocked interface (for example actor.Host or components.ActorProvider)
.PHONY: mocks
mocks:
	go tool mockery

.PHONY: gomod-age
gomod-age:
	go tool gomod-age
