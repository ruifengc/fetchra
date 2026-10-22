GO ?= go
BUILD_TAGS := nosqlite

.PHONY: build test race vet release
build:
	$(GO) build -tags $(BUILD_TAGS) -o bin/fetchra ./cmd/fetchra
test:
	$(GO) test -tags $(BUILD_TAGS) -timeout 120s ./...
race:
	$(GO) test -tags $(BUILD_TAGS) -race -timeout 180s ./...
vet:
	$(GO) vet -tags $(BUILD_TAGS) ./...
release: build
	python3 scripts/release.py
