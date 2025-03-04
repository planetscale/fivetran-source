package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	fivetran_sdk_v2 "github.com/planetscale/fivetran-source/fivetran_sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

var (
	addr       = flag.String("addr", "localhost:50051", "the address to connect to")
	tableName  = flag.String("table", "", "table to download")
	schemaName = flag.String("schema", "", "schema for table to download")
)

// fill in connection values
var configuration = map[string]string{
	"username": "",
	"password": "",
	"host":     "",
	"database": "",
}

func main() {
	flag.Parse()
	tailTable()
}

func getTableSchema(tableName string, configuration map[string]string) (*fivetran_sdk_v2.TableSelection, error) {
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	cc := fivetran_sdk_v2.NewSourceConnectorClient(conn)
	resp, err := cc.Schema(context.Background(), &fivetran_sdk_v2.SchemaRequest{Configuration: configuration})
	if err != nil {
		return nil, err
	}

	schemaList := resp.Response.(*fivetran_sdk_v2.SchemaResponse_WithSchema).WithSchema
	for _, schema := range schemaList.Schemas {
		for _, table := range schema.Tables {
			if table.Name == tableName {
				ts := &fivetran_sdk_v2.TableSelection{
					TableName: tableName,
					Included:  true,
					Columns:   make(map[string]bool),
				}

				for _, column := range table.Columns {
					ts.Columns[column.Name] = true
				}

				return ts, nil
			}
		}
	}

	return nil, nil
}

func tailTable() {
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	cc := fivetran_sdk_v2.NewSourceConnectorClient(conn)
	maxMsgSize := 16777216

	ts, err := getTableSchema(*tableName, configuration)
	if err != nil {
		log.Fatalf("Failed getting table schema with %v", err)
	}

	resp, err := cc.Update(context.Background(), &fivetran_sdk_v2.UpdateRequest{
		Configuration: configuration,
		Selection: &fivetran_sdk_v2.Selection{
			Selection: &fivetran_sdk_v2.Selection_WithSchema{
				WithSchema: &fivetran_sdk_v2.TablesWithSchema{
					Schemas: []*fivetran_sdk_v2.SchemaSelection{
						{
							Included:   true,
							SchemaName: *schemaName,
							Tables: []*fivetran_sdk_v2.TableSelection{
								ts,
							},
						},
					},
				},
			},
		},
	},
		grpc.UseCompressor(gzip.Name),
		grpc.MaxCallRecvMsgSize(maxMsgSize))
	if err != nil {
		log.Fatalf("Failed with %v", err)
	}

	for {
		res, err := resp.Recv()
		if err != nil {
			log.Fatalf("Failed with %v", err)
		}

		if log, ok := res.Operation.(*fivetran_sdk_v2.UpdateResponse_Task); ok {
			fmt.Println(log.Task.Message)
		}

		if operation, ok := res.Operation.(*fivetran_sdk_v2.UpdateResponse_Record); ok {
			rec := operation.Record
			fmt.Printf("[Record] : %v\n", rec)
		}
	}
}
