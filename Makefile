BIN_SERVER := serverstatus-server
BIN_CLIENT := serverstatus-client

GOOS   ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

.PHONY: build build-server build-client build-darwin-arm64 build-server-darwin-arm64 clean

build: build-server build-client build-darwin-arm64

build-server:
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_SERVER) server.go

build-server-darwin-arm64:
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_SERVER)-darwin-arm64 server.go

build-client:
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_CLIENT) client.go client_$(GOOS).go

build-darwin-arm64:
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_CLIENT)-darwin-arm64 client.go client_darwin.go

clean:
	rm -f $(BIN_SERVER) $(BIN_CLIENT) $(BIN_SERVER)-darwin-arm64 $(BIN_CLIENT)-darwin-arm64
