.PHONY: test
test:
	go test -tags unit ./...

.PHONY: test-race
test-race:
	CGO_ENABLED=1 go test -race -tags unit ./...

.PHONY: test-integration
test-integration:
	go test -tags integration -count=1 -timeout 15m ./tests/integration/...

.PHONY: lint lint-e2e
lint:
	golangci-lint run

lint-e2e:
	cd tests/e2e && golangci-lint run

# Regenerate the mocks in internal/mocks from the interfaces listed in .mockery.yml
# Run this after changing any mocked interface (for example actor.Host or components.ActorProvider)
.PHONY: mocks
mocks:
	go tool mockery

# Regenerate the management API's OpenAPI document from the swag annotations in internal/management
# swag writes Swagger 2.0, which openapi-convert turns into OpenAPI 3 in both JSON and YAML, so both run in a temporary directory and only the YAML the server embeds is kept
.PHONY: gen-openapi
gen-openapi:
	OPENAPI_TMPDIR="$$(mktemp -d)"; \
	trap 'rm -rf "$$OPENAPI_TMPDIR"' EXIT; \
	go tool swag init --quiet \
		--dir internal/management \
		--generalInfo doc.go \
		--markdownFiles internal/management/openapi \
		--output "$$OPENAPI_TMPDIR" --outputTypes json \
		--parseDependency --parseInternal --requiredByDefault && \
	go tool openapi-convert \
		-in "$$OPENAPI_TMPDIR/swagger.json" \
		-json "$$OPENAPI_TMPDIR/openapi.json" \
		-yaml "$$OPENAPI_TMPDIR/openapi.yaml" && \
	cat "$$OPENAPI_TMPDIR/openapi.yaml" > internal/management/openapi/openapi.yaml

# The generated OpenAPI document must stay in sync with the annotations it comes from
.PHONY: check-openapi-diff
check-openapi-diff: gen-openapi
	git diff --exit-code internal/management/openapi/openapi.yaml

.PHONY: gomod-age
gomod-age:
	go tool gomod-age
