BUILT_ON ?= $(shell date --rfc-3339=seconds | sed 's/ /T/')
BUILT_BY ?= $(shell whoami)
BUILD_REF ?= $(shell git symbolic-ref -q --short HEAD || git describe --tags --exact-match)

.PHONY: default
default: xyt

server/:
	mkdir -p $@

server/server.pb.go server/server_grpc.pb.go: **/*.proto | server/
	protoc --proto_path=proto --go_out=server --go-grpc_out=server --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative server.proto

xyt: *.go cmd/*.go cmd/**/*.go server/server.pb.go server/server_grpc.pb.go
	CGO_ENABLED=0 go build -ldflags="-s -w -X main.ref=$(BUILD_REF) -X main.buildUser=$(BUILT_BY) -X main.builtOn=$(BUILT_ON)" -trimpath -o $@
