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
# swag writes Swagger 2.0, whose shared responses let the Python script preserve JSON errors on binary endpoints before openapi-convert validates and writes OpenAPI 3
# Everything runs in a temporary directory and only the YAML the server embeds is kept
.PHONY: gen-openapi
gen-openapi:
	OPENAPI_TMPDIR="$$(mktemp -d)"; \
	trap 'rm -rf "$$OPENAPI_TMPDIR"' EXIT; \
	go tool swag init --quiet \
		--dir internal/management \
		--generalInfo doc.go \
		--markdownFiles internal/management/openapi \
		--overridesFile internal/management/openapi/overrides.swag \
		--output "$$OPENAPI_TMPDIR" --outputTypes json \
		--parseDependency --parseInternal --requiredByDefault && \
	python3 scripts/openapi-json-errors.py \
		"$$OPENAPI_TMPDIR/swagger.json" "$$OPENAPI_TMPDIR/swagger-json-errors.json" && \
	go tool openapi-convert \
		-in "$$OPENAPI_TMPDIR/swagger-json-errors.json" \
		-json "$$OPENAPI_TMPDIR/openapi.json" \
		-yaml "$$OPENAPI_TMPDIR/openapi.yaml" && \
	cat "$$OPENAPI_TMPDIR/openapi.yaml" > internal/management/openapi/openapi.yaml

# The generated OpenAPI document must stay in sync with the annotations it comes from
.PHONY: check-openapi-diff
check-openapi-diff: gen-openapi
	git diff --exit-code internal/management/openapi/openapi.yaml

# Build the management dashboard into dashboard/dist, which the runtime binary embeds
# Run this before building the runtime, or the binary serves the management API without the dashboard
.PHONY: dashboard
dashboard:
	cd dashboard && \
		pnpm install --frozen-lockfile && \
		pnpm run build

# Check the dashboard's formatting, lint rules, and types, then run its unit tests
.PHONY: dashboard-check
dashboard-check:
	cd dashboard && \
		pnpm run check:ci && \
		pnpm run svelte-check && \
		pnpm run test

# Run the dashboard's Playwright tests, against a runtime and a fixture host built from source
# Runs against chromium only by default
# Set E2E_BROWSERS to "all", or to a comma-separated list of "chromium", "firefox", and "webkit", to run against other engines
# Install the browsers once with "cd dashboard && pnpm run e2e:install"
.PHONY: dashboard-e2e
dashboard-e2e:
	cd dashboard && \
		pnpm run e2e

# Run a whole cluster with sample data in one process, for working on the dashboard
# Pass flags with DEMO_FLAGS, for example DEMO_FLAGS="-lease -verbose"
.PHONY: dashboard-demo
dashboard-demo:
	go run -tags demo ./cmd/runtime demo $(DEMO_FLAGS)

.PHONY: gomod-age
gomod-age:
	go tool gomod-age
