.PHONY: build run clean test test-unit test-integration test-bench test-chaos lint vet fmt check jepsen-build jepsen-run help

BINARY    := camu
CMD       := ./cmd/camu
BUILD_DIR := bin
GOFLAGS   := -trimpath
LDFLAGS   := -s -w

# Default config for local dev
CONFIG    ?= camu.yaml

## —— Build ——————————————————————————————————————————

build: ## Build the binary
	go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BUILD_DIR)/$(BINARY) $(CMD)

run: build ## Build and run the server
	$(BUILD_DIR)/$(BINARY) serve --config $(CONFIG)

clean: ## Remove build artifacts
	rm -rf $(BUILD_DIR)
	go clean -testcache

## —— Test ———————————————————————————————————————————

test: test-unit ## Run all non-integration tests (alias for test-unit)

test-unit: ## Run unit tests
	go test -race -count=1 ./internal/...

test-integration: ## Run integration tests (requires running MinIO/S3)
	go test -race -tags integration -timeout 300s -v ./test/integration/

test-bench: ## Run benchmarks
	go test -tags integration -bench=. -benchmem -timeout 300s ./test/bench/

test-chaos: ## Run chaos tests
	go test -race -tags integration,chaos -timeout 300s -v ./test/integration/

test-all: test-unit test-integration ## Run unit + integration tests

## —— Code Quality ———————————————————————————————————

lint: ## Run golangci-lint
	golangci-lint run ./...

vet: ## Run go vet
	go vet ./...

fmt: ## Format code
	gofmt -s -w .

check: vet lint ## Run all static checks (vet + lint)

## —— Jepsen —————————————————————————————————————————

jepsen-build: build ## Build binary for Jepsen and copy into place
	cp $(BUILD_DIR)/$(BINARY) jepsen/camu/camu

jepsen-run: jepsen-build ## Run Jepsen tests (requires Docker)
	cd jepsen/camu && bash run.sh

## —— Helpers ————————————————————————————————————————

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
