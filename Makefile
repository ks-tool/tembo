.PHONY: build

# The version is taken from git: the tag if the commit has one, else the short commit, with -dirty
# for a modified tree. Override with `make build VERSION=1.2.3`.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

LDFLAGS := -s -w -X github.com/ks-tool/tembo/internal/postgres.version=$(VERSION)

build:
	@CGO_ENABLED=0 go build -ldflags='$(LDFLAGS)' -trimpath -o bin/tembo main.go
