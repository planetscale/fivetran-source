package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	fivetransdk "github.com/planetscale/fivetran-sdk-grpc/go"
	"github.com/planetscale/fivetran-source/cmd/internal/server"
	"google.golang.org/grpc"
)

var port = flag.Int("port", 8000, "The server port")

func main() {
	ss := server.NewConnectorServer()
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	fivetransdk.RegisterConnectorServer(s, ss)
	fmt.Printf("server listening at %v\n", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
