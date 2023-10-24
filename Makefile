COMMIT := $(shell git rev-parse --short=7 HEAD 2>/dev/null)
VERSION := "0.1.19"
DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
NAME := "fivetran-source"
DOCKER_BUILD_PLATFORM := "linux/amd64"
DOCKER_LINUX_BUILD_PLATFORM := "linux/arm64/v8"
ifeq ($(strip $(shell git status --porcelain 2>/dev/null)),)
  GIT_TREE_STATE=clean
else
  GIT_TREE_STATE=dirty
endif

BIN := bin
export GOPRIVATE := github.com/planetscale/*
export GOBIN := $(PWD)/$(BIN)

GO ?= go
GO_ENV ?= PS_LOG_LEVEL=debug PS_DEV_MODE=1 CGO_ENABLED=0
GO_RUN := env $(GO_ENV) $(GO) run

OS := $(shell uname)
PROTOC_VERSION=3.20.1
PROTOC_ARCH=x86_64
ifeq ($(OS),Linux)
	PROTOC_PLATFORM := linux
endif
ifeq ($(OS),Darwin)
	PROTOC_PLATFORM := osx
endif
FIVETRANSDK_PROTO_OUT := proto/fivetransdk

.PHONY: all
all: build test lint-fmt lint

.PHONY: bootstrap
bootstrap:
	@go install mvdan.cc/gofumpt@latest

.PHONY: test
test:
	@go test ./...

.PHONY: build
build:
	@CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" ./...

.PHONY: fmt
fmt: bootstrap
	$(GOBIN)/gofumpt -w .

.PHONY: build-server
build-server:
	@CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -x ./cmd/server

.PHONY: server
server:
	@go run ./cmd/server/main.go -port 50051

.PHONY: lint
lint:
	@go install honnef.co/go/tools/cmd/staticcheck@latest
	@$(GOBIN)/staticcheck ./...

.PHONY: lint-fmt
lint-fmt: fmt
	git diff --exit-code

.PHONY: build-image
build-image:
	@echo "==> Building docker image ${REPO}/${NAME}:$(VERSION)"
	@docker build --platform ${DOCKER_BUILD_PLATFORM} --build-arg VERSION=$(VERSION:v%=%)  --build-arg GH_TOKEN=${GH_TOKEN} --build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) -t ${REPO}/${NAME}:$(VERSION) .
	@docker tag ${REPO}/${NAME}:$(VERSION) ${REPO}/${NAME}:latest

.PHONY: build-image-linux
build-image-linux:
	@echo "==> Building docker image ${REPO}/${NAME}:$(VERSION)"
	@docker build --platform ${DOCKER_LINUX_BUILD_PLATFORM} --build-arg VERSION=$(VERSION:v%=%)  --build-arg GH_TOKEN=${GH_TOKEN} --build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) -t ${REPO}/${NAME}:$(VERSION) .
	@docker tag ${REPO}/${NAME}:$(VERSION) ${REPO}/${NAME}:latest

.PHONY: push
push: build-image
	export REPO=$(REPO)
	@echo "==> Pushing docker image ${REPO}/${NAME}:$(VERSION)"
	@docker push ${REPO}/${NAME}:latest
	@docker push ${REPO}/${NAME}:$(VERSION)
	@echo "==> Your image is now available at $(REPO)/${NAME}:$(VERSION)"

.PHONY: clean
clean:
	@echo "==> Cleaning artifacts"
	@rm ${NAME}

proto: $(FIVETRANSDK_PROTO_OUT)/v1alpha1/fivetran_source.pb.go
$(BIN):
	mkdir -p $(BIN)

$(BIN)/protoc-gen-go: | $(BIN)
	go install google.golang.org/protobuf/cmd/protoc-gen-go

$(BIN)/protoc-gen-go-grpc: | $(BIN)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

$(BIN)/protoc-gen-go-vtproto: | $(BIN)
	go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto

$(BIN)/protoc-gen-twirp: | $(BIN)
	go install github.com/twitchtv/twirp/protoc-gen-twirp

$(BIN)/protoc: | $(BIN)
	rm -rf tmp-protoc
	mkdir -p tmp-protoc
	wget -O tmp-protoc/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-$(PROTOC_PLATFORM)-$(PROTOC_ARCH).zip
	unzip -d tmp-protoc tmp-protoc/protoc.zip
	mv tmp-protoc/bin/protoc $(BIN)/
	rm -rf thirdparty/google/
	mv tmp-protoc/include/google/ thirdparty/
	rm -rf tmp-protoc

PROTO_TOOLS := $(BIN)/protoc $(BIN)/protoc-gen-go $(BIN)/protoc-gen-go-grpc $(BIN)/protoc-gen-twirp

$(FIVETRANSDK_PROTO_OUT)/v1alpha1/fivetran_source.pb.go: $(PROTO_TOOLS) proto/fivetran_sdk.proto
	mkdir -p $(FIVETRANSDK_PROTO_OUT)/v1alpha1
	$(BIN)/protoc \
	  --plugin=protoc-gen-go=$(BIN)/protoc-gen-go \
	  --plugin=protoc-gen-go-grpc=$(BIN)/protoc-gen-go-grpc \
	  --go_out=$(FIVETRANSDK_PROTO_OUT)/v1alpha1 \
	  --go-grpc_out=$(FIVETRANSDK_PROTO_OUT)/v1alpha1 \
	  --go_opt=paths=source_relative \
	  --go-grpc_opt=paths=source_relative \
	  --go-grpc_opt=require_unimplemented_servers=false \
	  -I proto \
	  proto/fivetran_sdk.proto
