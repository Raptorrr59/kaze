.PHONY: proto build master worker kazectl certs clean

# Variables
PROTO_DIR = proto
MASTER_DIR = master
WORKER_DIR = worker
KAZECTL_DIR = kazectl
BIN_DIR = bin

# Tools
PROTOC = protoc
PROTOC_GEN_GO = $(shell go env GOPATH)/bin/protoc-gen-go
PROTOC_GEN_GO_GRPC = $(shell go env GOPATH)/bin/protoc-gen-go-grpc

all: build

proto:
	@echo "Generating gRPC code..."
	$(PROTOC) --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/kaze.proto

build: master worker kazectl

master:
	@echo "Building Master..."
	go build -o $(BIN_DIR)/kaze-master ./$(MASTER_DIR)

worker:
	@echo "Building Worker..."
	go build -o $(BIN_DIR)/kaze-worker ./$(WORKER_DIR)

kazectl:
	@echo "Building kazectl..."
	go build -o $(BIN_DIR)/kazectl ./$(KAZECTL_DIR)

run-master: build
	./$(BIN_DIR)/kaze-master

run-worker: build
	./$(BIN_DIR)/kaze-worker

certs:
	@echo "Generating certificates..."
	./scripts/gen-certs.sh

clean:
	rm -rf $(BIN_DIR)
	rm -rf certs
