package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/planetscale/fivetran-source/cmd/internal/server"
	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"google.golang.org/grpc"
)

var port = flag.Int("port", 50051, "The server port")

func main() {
	ss := server.NewConnectorServer()
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	fivetransdk.RegisterSourceConnectorServer(s, ss)
	fmt.Printf("server listening at %v\n", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
