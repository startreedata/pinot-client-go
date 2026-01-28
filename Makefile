# make file to hold the logic of build and test setup
PACKAGES := $(shell go list ./... | grep -v examples| grep -v integration-tests)
INTEGRATION_TESTS_PACKAGES := $(shell go list ./... | grep integration-tests)

.DEFAULT_GOAL := test

.PHONY: install-covertools
install-covertools:
	go get github.com/mattn/goveralls
	go get golang.org/x/tools/cmd/cover

.PHONY: install-deps
install-deps:
	go get github.com/go-zookeeper/zk
	go get github.com/sirupsen/logrus
	go get github.com/stretchr/testify/assert
	go get github.com/stretchr/testify/mock

.PHONY: setup
setup: install-covertools install-deps

.PHONY: lint
lint:
	go fmt ./...
	go vet ./...
	@# Use golangci-lint v2 (matches CI). If a v2 binary isn't installed, run via `go run`.
	@set -euo pipefail; \
	if command -v golangci-lint >/dev/null 2>&1 && golangci-lint version 2>/dev/null | grep -q 'version 2\.'; then \
		golangci-lint run ./...; \
	else \
		go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.8.0 run ./...; \
	fi

.PHONY: build
build:
	go build ./...

.PHONY: test
test: build
	go test -timeout 500s -v -race -covermode atomic -coverprofile=coverage.out $(PACKAGES)

.PHONY: run-pinot-dist
run-pinot-dist:
	./scripts/start-pinot-quickstart.sh

.PHONY: stop-pinot-dist
stop-pinot-dist:
	./scripts/stop-pinot-quickstart.sh

.PHONY: run-pinot-docker
run-pinot-docker:
	docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 -p 8010:8010 apachepinot/pinot:latest QuickStart -type MULTI_STAGE

.PHONY: stop-pinot-docker
stop-pinot-docker:
	docker stop pinot-quickstart || true
	docker rm pinot-quickstart || true

.PHONY: integration-test
integration-test: build
	go test -timeout 500s -v -race -covermode atomic -coverprofile=coverage.out $(INTEGRATION_TESTS_PACKAGES)

.PHONY: coverage-check
coverage-check: test
	./scripts/check-coverage.sh

.PHONY: coverage-baseline
coverage-baseline: test
	./scripts/update-coverage-baseline.sh

.PHONY: hooks
hooks:
	git config core.hooksPath .githooks
