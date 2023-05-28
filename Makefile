# make file to hold the logic of build and test setup
PACKAGES := $(shell go list ./... | grep -v examples)

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

.PHONY: setup
setup: install-covertools install-deps

.PHONY: lint
lint:
	go fmt ./...
	go vet ./...
	golangci-lint run

.PHONY: build
build:
	go build ./...

.PHONY: test
test: build
	go test -timeout 500s -v -race -covermode atomic -coverprofile=profile.cov $(PACKAGES)

