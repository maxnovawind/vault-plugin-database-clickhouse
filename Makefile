SHELL := /usr/bin/env bash -euo pipefail -c

# Determine this makefile's path.
# Be sure to place this BEFORE `include` directives, if any.
THIS_FILE := $(lastword $(MAKEFILE_LIST))

TEST?=$$(go list ./... | grep -v /vendor/ | grep -v /integ)
GOFMT_FILES?=$$(find . -name '*.go' | grep -v vendor)
GO_MOD_TIDY ?= GO111MODULE=on go mod tidy -go=1.17

default: dev


install-deps:
	${GO_MOD_TIDY}

# bin generates the releaseable binaries for vault-plugin-database-clickhouse
build: fmtcheck install-deps
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/vault-clickhouse-database-plugin ./cmd/vault-plugin-database-clickhouse

dev: fmtcheck generate
	@CGO_ENABLED=1 BUILD_TAGS='$(BUILD_TAGS)' VAULT_DEV_BUILD=1 sh -c "'$(CURDIR)/scripts/build.sh'"

# test runs the unit tests and vets the code
test: fmtcheck generate
	CGO_ENABLED=1 go test ./... -coverprofile cover.out -timeout=20m -parallel=4

# generate runs `go generate` to build the dynamically generated
# source files.
generate:
	go generate $(go list ./... | grep -v /vendor/)

fmtcheck:
	@sh -c "'$(CURDIR)/scripts/gofmtcheck.sh'"

fmt:
	gofmt -w $(GOFMT_FILES)

run:
	./scripts/local_dev.sh

# bootstrap the build by downloading additional tools
bootstrap:
	@for tool in  $(EXTERNAL_TOOLS) ; do \
		echo "Installing/Updating $$tool" ; \
		go install $$tool; \
	done

.PHONY: bin default generate test fmt fmtcheck dev bootstrap