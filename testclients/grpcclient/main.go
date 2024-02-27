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
var tableName = flag.String("table", "", "table to download")
var schemaName = flag.String("schema", "", "schema for table to download")

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

func getTableSchema(tableName string, configuration map[string]string) (*fivetran_sdk.TableSelection, error) {
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	cc := fivetran_sdk.NewConnectorClient(conn)
	resp, err := cc.Schema(context.Background(), &fivetran_sdk.SchemaRequest{Configuration: configuration})
	if err != nil {
		return nil, err
	}

	schemaList := resp.Response.(*fivetran_sdk.SchemaResponse_WithSchema).WithSchema
	for _, schema := range schemaList.Schemas {
		for _, table := range schema.Tables {
			if table.Name == tableName {
				ts := &fivetran_sdk.TableSelection{
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
	cc := fivetran_sdk.NewConnectorClient(conn)
	maxMsgSize := 16777216

	ts, err := getTableSchema(*tableName, configuration)
	if err != nil {
		log.Fatalf("Failed getting table schema with %v", err)
	}

	resp, err := cc.Update(context.Background(), &fivetran_sdk.UpdateRequest{
		Configuration: configuration,
		Selection: &fivetran_sdk.Selection{
			Selection: &fivetran_sdk.Selection_WithSchema{
				WithSchema: &fivetran_sdk.TablesWithSchema{
					Schemas: []*fivetran_sdk.SchemaSelection{
						&fivetran_sdk.SchemaSelection{
							Included:   true,
							SchemaName: *schemaName,
							Tables: []*fivetran_sdk.TableSelection{
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

		if log, ok := res.Response.(*fivetran_sdk.UpdateResponse_LogEntry); ok {
			fmt.Println(log.LogEntry.Message)
		}

		if operation, ok := res.Response.(*fivetran_sdk.UpdateResponse_Operation); ok {
			//operation.Operation.Op
			if rec, ok := operation.Operation.Op.(*fivetran_sdk.Operation_Record); ok {

				fmt.Printf("[Record] : %v\n", rec.Record)
			}

		}
	}
}
