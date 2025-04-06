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

.PHONY: build
build:
	go build ./...

.PHONY: test
test: build
	go test -timeout 500s -v -race -covermode atomic -coverprofile=coverage.out $(PACKAGES)

.PHONY: run-pinot-dist
run-pinot-dist:
	./scripts/start-pinot-quickstart.sh

.PHONY: run-pinot-docker
run-pinot-docker:
	docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 apachepinot/pinot:latest QuickStart -type MULTI_STAGE

.PHONY: integration-test
integration-test: build
	go test -timeout 500s -v -race -covermode atomic -coverprofile=coverage.out $(INTEGRATION_TESTS_PACKAGES)
