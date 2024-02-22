package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/planetscale/fivetran-source/fivetran_sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

var addr = flag.String("addr", "localhost:50051", "the address to connect to")

func main() {
	flag.Parse()
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	cc := fivetran_sdk.NewConnectorClient(conn)
	resp, err := cc.ConfigurationForm(context.Background(), &fivetran_sdk.ConfigurationFormRequest{}, grpc.UseCompressor(gzip.Name))
	if err != nil {
		log.Fatalf("Failed with %v", err)
	}

	fmt.Printf("\\n\t receieved response : %v", resp)
}
