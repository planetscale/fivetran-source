package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"testing"

	"github.com/planetscale/fivetran-source/lib"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"github.com/stretchr/testify/require"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/fivetran-source/cmd/internal/server/handlers"

	"vitess.io/vitess/go/sqltypes"

	fivetransdk "github.com/planetscale/fivetran-proto/go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func server(ctx context.Context, clientConstructor edgeClientConstructor, mysqlConstructor mysqlClientConstructor) (fivetransdk.ConnectorClient, func()) {
	buffer := 101024 * 1024
	lis := bufconn.Listen(buffer)

	baseServer := grpc.NewServer()
	cs := NewConnectorServer()
	cs.(*connectorServer).clientConstructor = clientConstructor
	cs.(*connectorServer).mysqlClientConstructor = mysqlConstructor
	fivetransdk.RegisterConnectorServer(baseServer, cs)
	go func() {
		if err := baseServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()
	conn, err := grpc.DialContext(ctx, "",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("error connecting to server: %v", err)
	}

	closer := func() {
		err := lis.Close()
		if err != nil {
			log.Printf("error closing listener: %v", err)
		}
		baseServer.Stop()
	}

	client := fivetransdk.NewConnectorClient(conn)

	return client, closer
}

func TestCanCallConfigurationForm(t *testing.T) {
	ctx := context.Background()

	client, closer := server(ctx, nil, nil)
	defer closer()

	out, err := client.ConfigurationForm(ctx, &fivetransdk.ConfigurationFormRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, out)

	assert.True(t, out.TableSelectionSupported)
	assert.True(t, out.SchemaSelectionSupported)
}

func TestUpdateValidatesConfiguration(t *testing.T) {
	ctx := context.Background()

	client, closer := server(ctx, nil, nil)
	defer closer()
	out, err := client.Update(ctx, &fivetransdk.UpdateRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, out)

	_, err = out.Recv()
	assert.ErrorContains(t, err, "request did not contain a valid configuration")
}

func TestUpdateValidatesSchemaSelection(t *testing.T) {
	ctx := context.Background()

	client, closer := server(ctx, nil, nil)
	defer closer()
	out, err := client.Update(ctx, &fivetransdk.UpdateRequest{
		Configuration: map[string]string{
			"host":     "earth.psdb",
			"username": "phanatic",
			"password": "password",
			"database": "employees",
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, out)

	_, err = out.Recv()
	assert.ErrorContains(t, err, "request did not contain a valid selection")
}

func TestUpdateValidatesState(t *testing.T) {
	ctx := context.Background()
	mysqlClientConstructor := func() lib.MysqlClient {
		return &lib.TestMysqlClient{
			BuildSchemaFn: func(ctx context.Context, psc lib.PlanetScaleSource, schemaBuilder lib.SchemaBuilder) error {
				schemaBuilder.OnKeyspace("SalesDB")
				schemaBuilder.OnTable("SalesDB", "customers")
				return nil
			},
		}
	}
	clientConstructor := func() lib.ConnectClient {
		return &lib.TestConnectClient{
			ListShardsFn: func(ctx context.Context, ps lib.PlanetScaleSource) ([]string, error) {
				return []string{"-"}, nil
			},
		}
	}

	client, closer := server(ctx, clientConstructor, mysqlClientConstructor)
	defer closer()
	invalidJSON := "{name: value"
	out, err := client.Update(ctx, &fivetransdk.UpdateRequest{
		Configuration: map[string]string{
			"host":     "earth.psdb",
			"username": "phanatic",
			"password": "password",
			"database": "employees",
		},
		Selection: &fivetransdk.Selection{
			Selection: &fivetransdk.Selection_WithSchema{
				WithSchema: &fivetransdk.TablesWithSchema{
					Schemas: []*fivetransdk.SchemaSelection{},
				},
			},
		},
		StateJson: &invalidJSON,
	})
	assert.NoError(t, err)
	assert.NotNil(t, out)

	_, err = out.Recv()
	assert.ErrorContains(t, err, "request did not contain a valid stateJson")
}

func TestUpdateReturnsInserts(t *testing.T) {
	ctx := context.Background()
	intValue := strconv.AppendInt(nil, int64(int8(12)), 10)
	allTypesResult, mysqlClientConstructor := setupUpdateRowsTest(intValue)

	clientConstructor := func() lib.ConnectClient {
		return &lib.TestConnectClient{
			ListShardsFn: func(ctx context.Context, ps lib.PlanetScaleSource) ([]string, error) {
				return []string{"-", "-40"}, nil
			},
			CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
				return nil
			},
			ReadFn: func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string, tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor) (*lib.SerializedCursor, error) {
				assert.Equal(t, "customers", tableName)
				assert.NotNil(t, columns)
				onResult(allTypesResult, lib.OpType_Insert)
				return nil, nil
			},
		}
	}
	client, closer := server(ctx, clientConstructor, mysqlClientConstructor)
	defer closer()
	customerSelection := &fivetransdk.TableSelection{
		Included:  true,
		TableName: "customers",
		Columns:   map[string]bool{},
	}

	for _, f := range allTypesResult.Fields {
		customerSelection.Columns[f.Name] = true
	}

	selection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{
			Schemas: []*fivetransdk.SchemaSelection{
				{
					SchemaName: "SalesDB",
					Included:   true,
					Tables: []*fivetransdk.TableSelection{
						customerSelection,
						{
							Included:  false,
							TableName: "customer_secrets",
						},
					},
				},
			},
		},
	}
	out, err := client.Update(ctx, &fivetransdk.UpdateRequest{
		Configuration: map[string]string{
			"host":     "earth.psdb",
			"username": "phanatic",
			"password": "password",
			"database": "employees",
		},
		Selection: &fivetransdk.Selection{
			Selection: selection,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, out)

	rows := make([]*fivetransdk.UpdateResponse, 0, 3)
	for {
		resp, err := out.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("failed test with %q", err)
		}
		rows = append(rows, resp)
	}
	assert.Len(t, rows, 3)
	operation := rows[0].GetOperation()
	assert.NotNil(t, operation)
	record, ok := operation.Op.(*fivetransdk.Operation_Record)
	assert.True(t, ok)
	assert.NotNil(t, record)
	assert.Equal(t, "SalesDB", *record.Record.SchemaName)
	assert.Equal(t, "customers", record.Record.TableName)

	assert.Equal(t, record.Record.Type, fivetransdk.OpType_UPSERT)
	for _, field := range allTypesResult.Fields {
		assert.NotEmpty(t, record.Record.Data[field.Name].Inner, "expected value for %q field", field.Name)
		assert.NotNil(t, record.Record.Data[field.Name].Inner, "expected value for %q field", field.Name)
	}

	operation = rows[len(rows)-1].GetOperation()
	checkpoint, ok := operation.Op.(*fivetransdk.Operation_Checkpoint)
	assert.True(t, ok)
	assert.NotNil(t, checkpoint)

	syncState := lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{},
	}

	err = json.Unmarshal([]byte(checkpoint.Checkpoint.StateJson), &syncState)
	require.NoError(t, err)

	ks, ok := syncState.Keyspaces["SalesDB"]
	assert.True(t, ok)
	customers, ok := ks.Streams["SalesDB:customers"]
	assert.True(t, ok)
	defaultShard, ok := customers.Shards["-"]
	assert.True(t, ok)
	assert.Equal(t, "CgEtEgdTYWxlc0RC", defaultShard.Cursor)

	customShard, ok := customers.Shards["-40"]
	assert.True(t, ok)
	assert.Equal(t, "CgMtNDASB1NhbGVzREI=", customShard.Cursor)
}

func TestUpdateReturnsDeletes(t *testing.T) {
	ctx := context.Background()
	intValue := strconv.AppendInt(nil, int64(int8(12)), 10)
	allTypesResult, mysqlClientConstructor := setupUpdateRowsTest(intValue)

	clientConstructor := func() lib.ConnectClient {
		return &lib.TestConnectClient{
			ListShardsFn: func(ctx context.Context, ps lib.PlanetScaleSource) ([]string, error) {
				return []string{"-", "-40"}, nil
			},
			CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
				return nil
			},
			ReadFn: func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string, tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor) (*lib.SerializedCursor, error) {
				assert.Equal(t, "customers", tableName)
				assert.NotNil(t, columns)
				onResult(allTypesResult, lib.OpType_Delete)
				return nil, nil
			},
		}
	}
	client, closer := server(ctx, clientConstructor, mysqlClientConstructor)
	defer closer()
	customerSelection := &fivetransdk.TableSelection{
		Included:  true,
		TableName: "customers",
		Columns:   map[string]bool{},
	}

	for _, f := range allTypesResult.Fields {
		customerSelection.Columns[f.Name] = true
	}

	selection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{
			Schemas: []*fivetransdk.SchemaSelection{
				{
					SchemaName: "SalesDB",
					Included:   true,
					Tables: []*fivetransdk.TableSelection{
						customerSelection,
						{
							Included:  false,
							TableName: "customer_secrets",
						},
					},
				},
			},
		},
	}
	out, err := client.Update(ctx, &fivetransdk.UpdateRequest{
		Configuration: map[string]string{
			"host":     "earth.psdb",
			"username": "phanatic",
			"password": "password",
			"database": "employees",
		},
		Selection: &fivetransdk.Selection{
			Selection: selection,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, out)

	rows := make([]*fivetransdk.UpdateResponse, 0, 3)
	for {
		resp, err := out.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("failed test with %q", err)
		}
		rows = append(rows, resp)
	}
	assert.Len(t, rows, 3)
	operation := rows[0].GetOperation()
	assert.NotNil(t, operation)
	record, ok := operation.Op.(*fivetransdk.Operation_Record)
	assert.True(t, ok)
	assert.NotNil(t, record)
	assert.Equal(t, "SalesDB", *record.Record.SchemaName)
	assert.Equal(t, "customers", record.Record.TableName)

	assert.Equal(t, record.Record.Type, fivetransdk.OpType_DELETE)
	for _, field := range allTypesResult.Fields {
		assert.NotEmpty(t, record.Record.Data[field.Name].Inner, "expected value for %q field", field.Name)
		assert.NotNil(t, record.Record.Data[field.Name].Inner, "expected value for %q field", field.Name)
	}

	operation = rows[len(rows)-1].GetOperation()
	checkpoint, ok := operation.Op.(*fivetransdk.Operation_Checkpoint)
	assert.True(t, ok)
	assert.NotNil(t, checkpoint)

	syncState := lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{},
	}

	err = json.Unmarshal([]byte(checkpoint.Checkpoint.StateJson), &syncState)
	require.NoError(t, err)

	ks, ok := syncState.Keyspaces["SalesDB"]
	assert.True(t, ok)
	customers, ok := ks.Streams["SalesDB:customers"]
	assert.True(t, ok)
	defaultShard, ok := customers.Shards["-"]
	assert.True(t, ok)
	assert.Equal(t, "CgEtEgdTYWxlc0RC", defaultShard.Cursor)

	customShard, ok := customers.Shards["-40"]
	assert.True(t, ok)
	assert.Equal(t, "CgMtNDASB1NhbGVzREI=", customShard.Cursor)
}

func setupUpdateRowsTest(intValue []byte) (*sqltypes.Result, func() lib.MysqlClient) {
	allTypesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Type_INT8", Type: querypb.Type_INT8},
			{Name: "Type_UINT8", Type: querypb.Type_UINT8},
			{Name: "Type_INT16", Type: querypb.Type_INT16},
			{Name: "Type_UINT16", Type: querypb.Type_UINT16},
			{Name: "Type_INT24", Type: querypb.Type_INT24},
			{Name: "Type_UINT24", Type: querypb.Type_UINT24},
			{Name: "Type_INT32", Type: querypb.Type_INT32},
			{Name: "Type_UINT32", Type: querypb.Type_UINT32},
			{Name: "Type_INT64", Type: querypb.Type_INT64},
			{Name: "Type_UINT64", Type: querypb.Type_UINT64},
			{Name: "Type_FLOAT32", Type: querypb.Type_FLOAT32},
			{Name: "Type_FLOAT64", Type: querypb.Type_FLOAT64},
			{Name: "Type_TIMESTAMP", Type: querypb.Type_TIMESTAMP},
			{Name: "Type_DATE", Type: querypb.Type_DATE},
			{Name: "Type_TIME", Type: querypb.Type_TIME},
			{Name: "Type_DATETIME", Type: querypb.Type_DATETIME},
			{Name: "Type_YEAR", Type: querypb.Type_YEAR},
			{Name: "Type_DECIMAL", Type: querypb.Type_DECIMAL},
			{Name: "Type_TEXT", Type: querypb.Type_TEXT},
			{Name: "Type_BLOB", Type: querypb.Type_BLOB},
			{Name: "Type_VARCHAR", Type: querypb.Type_VARCHAR},
			{Name: "Type_VARBINARY", Type: querypb.Type_VARBINARY},
			{Name: "Type_CHAR", Type: querypb.Type_CHAR},
			{Name: "Type_BINARY", Type: querypb.Type_BINARY},
			{Name: "Type_BIT", Type: querypb.Type_BIT},
			{Name: "Type_ENUM", Type: querypb.Type_ENUM},
			{Name: "Type_SET", Type: querypb.Type_SET},
			// Skip TUPLE, not possible in Result.
			{Name: "Type_GEOMETRY", Type: querypb.Type_GEOMETRY},
			{Name: "Type_JSON", Type: querypb.Type_JSON},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT8, intValue),
				sqltypes.MakeTrusted(querypb.Type_UINT8, intValue),
				sqltypes.MakeTrusted(querypb.Type_INT16, intValue),
				sqltypes.MakeTrusted(querypb.Type_UINT16, intValue),
				sqltypes.MakeTrusted(querypb.Type_INT24, intValue),
				sqltypes.MakeTrusted(querypb.Type_UINT24, intValue),
				sqltypes.MakeTrusted(querypb.Type_INT32, intValue),
				sqltypes.MakeTrusted(querypb.Type_UINT32, intValue),
				sqltypes.MakeTrusted(querypb.Type_INT64, intValue),
				sqltypes.MakeTrusted(querypb.Type_UINT64, intValue),
				sqltypes.MakeTrusted(querypb.Type_FLOAT32, []byte("1.00")),
				sqltypes.MakeTrusted(querypb.Type_FLOAT64, []byte("1.00")),
				sqltypes.MakeTrusted(querypb.Type_TIMESTAMP, []byte("2006-01-02 15:04:05")),
				sqltypes.MakeTrusted(querypb.Type_DATE, []byte("2023-03-24")),
				sqltypes.MakeTrusted(querypb.Type_TIME, []byte("Type_TIME")),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2023-03-23 14:28:21.592111")),
				sqltypes.MakeTrusted(querypb.Type_YEAR, []byte("2023")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("Type_DECIMAL")),
				sqltypes.MakeTrusted(querypb.Type_TEXT, []byte("Type_TEXT")),
				sqltypes.MakeTrusted(querypb.Type_BLOB, []byte("Type_BLOB")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("Type_VARCHAR")),
				sqltypes.MakeTrusted(querypb.Type_VARBINARY, []byte("Type_VARBINARY")),
				sqltypes.MakeTrusted(querypb.Type_CHAR, []byte("Type_CHAR")),
				sqltypes.MakeTrusted(querypb.Type_BINARY, []byte("Type_BINARY")),
				sqltypes.MakeTrusted(querypb.Type_BIT, []byte{1}),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_SET, []byte{0x01, 0x02}),
				sqltypes.MakeTrusted(querypb.Type_GEOMETRY, []byte("Type_GEOMETRY")),
				sqltypes.MakeTrusted(querypb.Type_JSON, []byte("Type_JSON")),
			},
		},
	}

	mysqlClientConstructor := func() lib.MysqlClient {
		return &lib.TestMysqlClient{
			BuildSchemaFn: func(ctx context.Context, psc lib.PlanetScaleSource, schemaBuilder lib.SchemaBuilder) error {
				schemaBuilder.OnKeyspace("SalesDB")
				schemaBuilder.OnTable("SalesDB", "customers")
				schemaBuilder.OnColumns("SalesDB", "customers",
					[]lib.MysqlColumn{
						{Name: "Type_INT8", Type: "int"},
						{Name: "Type_UINT8", Type: "smallint"},
						{Name: "Type_INT16", Type: "smallint"},
						{Name: "Type_UINT16", Type: "int"},
						{Name: "Type_INT24", Type: "int"},
						{Name: "Type_UINT24", Type: "int"},
						{Name: "Type_INT32", Type: "int"},
						{Name: "Type_UINT32", Type: "unsigned int"},
						{Name: "Type_INT64", Type: "bigint"},
						{Name: "Type_UINT64", Type: "unsigned bigint"},
						{Name: "Type_FLOAT32", Type: "float"},
						{Name: "Type_FLOAT64", Type: "double"},
						{Name: "Type_TIMESTAMP", Type: "timestamp"},
						{Name: "Type_DATE", Type: "date"},
						{Name: "Type_TIME", Type: "time"},
						{Name: "Type_DATETIME", Type: "datetime"},
						{Name: "Type_YEAR", Type: "year"},
						{Name: "Type_DECIMAL", Type: "decimal"},
						{Name: "Type_TEXT", Type: "varchar"},
						{Name: "Type_BLOB", Type: "blob"},
						{Name: "Type_VARCHAR", Type: "varchar"},
						{Name: "Type_VARBINARY", Type: "geometry"},
						{Name: "Type_CHAR", Type: "char"},
						{Name: "Type_BINARY", Type: "binary"},
						{Name: "Type_BIT", Type: "bit"},
						{Name: "Type_ENUM", Type: "enum"},
						{Name: "Type_SET", Type: "set"},
						// Skip TUPLE, not possible in Result.
						{Name: "Type_GEOMETRY", Type: "geometry"},
						{Name: "Type_JSON", Type: "json"},
					})
				return nil
			},
		}
	}
	return allTypesResult, mysqlClientConstructor
}

func TestUpdateReturnsState(t *testing.T) {
	ctx := context.Background()
	mysqlClientConstructor := func() lib.MysqlClient {
		return &lib.TestMysqlClient{
			BuildSchemaFn: func(ctx context.Context, psc lib.PlanetScaleSource, schemaBuilder lib.SchemaBuilder) error {
				schemaBuilder.OnKeyspace("SalesDB")
				schemaBuilder.OnTable("SalesDB", "customers")
				return nil
			},
		}
	}
	clientConstructor := func() lib.ConnectClient {
		return &lib.TestConnectClient{
			ListShardsFn: func(ctx context.Context, ps lib.PlanetScaleSource) ([]string, error) {
				return []string{"-"}, nil
			},
			CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
				return nil
			},
			ReadFn: func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string, tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor) (*lib.SerializedCursor, error) {
				onCursor(&psdbconnect.TableCursor{
					Position: "THIS_IS_A_VALID_GTID",
				})
				return nil, nil
			},
		}
	}
	client, closer := server(ctx, clientConstructor, mysqlClientConstructor)
	defer closer()
	out, err := client.Update(ctx, &fivetransdk.UpdateRequest{
		Configuration: map[string]string{
			"host":     "earth.psdb",
			"username": "phanatic",
			"password": "password",
			"database": "employees",
		},
		Selection: &fivetransdk.Selection{
			Selection: &fivetransdk.Selection_WithSchema{
				WithSchema: &fivetransdk.TablesWithSchema{
					Schemas: []*fivetransdk.SchemaSelection{
						{
							SchemaName: "SalesDB",
							Included:   true,
							Tables: []*fivetransdk.TableSelection{
								{
									Included:  true,
									TableName: "customers",
								},
								{
									Included:  false,
									TableName: "customer_secrets",
								},
							},
						},
					},
				},
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, out)

	rows := make([]*fivetransdk.UpdateResponse, 0, 2)
	for {
		resp, err := out.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			t.Fatalf("failed test with %q", err)
		}
		rows = append(rows, resp)
	}
	assert.Len(t, rows, 2)
	operation := rows[0].GetOperation()
	assert.NotNil(t, operation)
	checkpoint, ok := operation.Op.(*fivetransdk.Operation_Checkpoint)
	require.True(t, ok)
	assert.NotNil(t, checkpoint)
	assert.Equal(t, "{\"keyspaces\":{\"SalesDB\":{\"streams\":{\"SalesDB:customers\":{\"shards\":{\"-\":{\"cursor\":\"GhRUSElTX0lTX0FfVkFMSURfR1RJRA==\"}}}}}}}", checkpoint.Checkpoint.StateJson)
}

func TestCheckConnectionReturnsSuccess(t *testing.T) {
	ctx := context.Background()
	clientConstructor := func() lib.ConnectClient {
		return &lib.TestConnectClient{
			CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
				return nil
			},
		}
	}
	client, closer := server(ctx, clientConstructor, nil)
	defer closer()
	resp, err := client.Test(ctx, &fivetransdk.TestRequest{
		Name: handlers.CheckConnectionTestName,
		Configuration: map[string]string{
			"host":     "earth.psdb",
			"username": "phanatic",
			"password": "password",
			"database": "employees",
		},
	})

	assert.NoError(t, err)
	success, ok := resp.Response.(*fivetransdk.TestResponse_Success)
	assert.True(t, ok, "response should be a TestResponse_Success")
	assert.True(t, success.Success, "response should be a success status")
}

func TestCheckConnectionReturnsErrorIfNotEdgePassword(t *testing.T) {
	ctx := context.Background()
	client, closer := server(ctx, nil, nil)
	defer closer()
	resp, err := client.Test(ctx, &fivetransdk.TestRequest{
		Name: handlers.CheckConnectionTestName,
		Configuration: map[string]string{
			"host":     "earth.psdb",
			"username": "phanatic",
			"password": "password",
			"database": "employees",
		},
	})

	assert.NoError(t, err)
	fail, ok := resp.Response.(*fivetransdk.TestResponse_Failure)
	assert.True(t, ok, "response should be a TestResponse_Failure")
	assert.Equal(t, fail.Failure, "Unable to initialize Connect Session: This password is not connect-enabled, please ensure that your organization is enrolled in the Connect beta.")
}

func TestCheckConnectionReturnsErrorIfCheckFails(t *testing.T) {
	ctx := context.Background()
	clientConstructor := func() lib.ConnectClient {
		return &lib.TestConnectClient{
			CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
				return fmt.Errorf("unable to connect to PlanetScale Database : %v", ps.Database)
			},
		}
	}
	client, closer := server(ctx, clientConstructor, nil)
	defer closer()
	resp, err := client.Test(ctx, &fivetransdk.TestRequest{
		Name: handlers.CheckConnectionTestName,
		Configuration: map[string]string{
			"host":     "earth.psdb",
			"username": "phanatic",
			"password": "password",
			"database": "employees",
		},
	})

	assert.NoError(t, err)
	fail, ok := resp.Response.(*fivetransdk.TestResponse_Failure)
	assert.True(t, ok, "response should be a TestResponse_Failure")
	assert.Equal(t, fail.Failure, "unable to connect to PlanetScale Database : employees")
}

func TestSchemaChecksCredentials(t *testing.T) {
	ctx := context.Background()
	clientConstructor := func() lib.ConnectClient {
		return &lib.TestConnectClient{
			CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
				return fmt.Errorf("access denied for user : %v", ps.Username)
			},
		}
	}
	client, closer := server(ctx, clientConstructor, nil)
	defer closer()
	_, err := client.Schema(ctx, &fivetransdk.SchemaRequest{
		Configuration: map[string]string{
			"host":     "earth.psdb",
			"username": "phanatic",
			"password": "password",
			"database": "employees",
		},
	})
	assert.ErrorContains(t, err, "unable to connect to PlanetScale database")
}
