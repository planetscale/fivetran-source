//go:build tools

package tools

// These imports ensure that "go mod tidy" won't remove deps
// for build-time dependencies like linters and code generators
import (
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)
