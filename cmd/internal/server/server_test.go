package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/planetscale/fivetran-source/lib"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"github.com/stretchr/testify/require"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/fivetran-source/cmd/internal/server/handlers"

	"vitess.io/vitess/go/sqltypes"

	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func server(ctx context.Context, clientConstructor edgeClientConstructor, mysqlConstructor mysqlClientConstructor) (fivetransdk.SourceConnectorClient, func()) {
	buffer := 101024 * 1024
	lis := bufconn.Listen(buffer)

	baseServer := grpc.NewServer()
	cs := NewConnectorServer()
	cs.(*connectorServer).clientConstructor = clientConstructor
	cs.(*connectorServer).mysqlClientConstructor = mysqlConstructor
	fivetransdk.RegisterSourceConnectorServer(baseServer, cs)
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

	client := fivetransdk.NewSourceConnectorClient(conn)

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

func TestCanSerializeGeometryTypes(t *testing.T) {
	tests := []struct {
		Type string
		Hex  string
		Json string
	}{
		{
			Type: "POINT",
			// Select ST_GeomFromText("POINT(-112.8185647 49.6999986)")
			Hex:  "0000000001010000003E0A325D63345CC0761FDB8D99D94840",
			Json: "{\"type\":\"Point\",\"coordinates\":[-112.8185647,49.6999986]}",
		},
		{
			Type: "LINESTRING",
			// Select ST_GeomFromText("LINESTRING(0 0, 10 10, 20 25, 50 60)")
			Hex:  "0000000001020000000400000000000000000000000000000000000000000000000000244000000000000024400000000000003440000000000000394000000000000049400000000000004E40",
			Json: "{\"type\":\"LineString\",\"coordinates\":[[0,0],[10,10],[20,25],[50,60]]}",
		},
		{
			Type: "POLYGON",
			// Select ST_GeomFromText("POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))")
			Hex:  "0000000001030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000005000000000000000000144000000000000014400000000000001C4000000000000014400000000000001C400000000000001C4000000000000014400000000000001C4000000000000014400000000000001440",
			Json: "{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[10,0],[10,10],[0,10],[0,0]],[[5,5],[7,5],[7,7],[5,7],[5,5]]]}",
		},
		{
			Type: "MULTIPOINT",
			// Select ST_GeomFromText("MULTIPOINT(0 0, 20 20, 60 60)")
			Hex:  "0000000001040000000300000001010000000000000000000000000000000000000001010000000000000000003440000000000000344001010000000000000000004E400000000000004E40",
			Json: "{\"type\":\"MultiPoint\",\"coordinates\":[[0,0],[20,20],[60,60]]}",
		},
		{
			Type: "MULTILINESTRING",
			// ST_GeomFromText("MULTILINESTRING((10 10, 20 20), (15 15, 30 15))")
			Hex:  "0000000001050000000200000001020000000200000000000000000024400000000000002440000000000000344000000000000034400102000000020000000000000000002E400000000000002E400000000000003E400000000000002E40",
			Json: "{\"type\":\"MultiLineString\",\"coordinates\":[[[10,10],[20,20]],[[15,15],[30,15]]]}",
		},
		{
			Type: "MULTIPOLYGON",
			// ST_GeomFromText("MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7, 5 5)))")
			Hex:  "0000000001060000000200000001030000000100000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000001030000000100000005000000000000000000144000000000000014400000000000001C4000000000000014400000000000001C400000000000001C4000000000000014400000000000001C4000000000000014400000000000001440",
			Json: "{\"type\":\"MultiPolygon\",\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]]}",
		},
		{
			Type: "GEOMETRYCOLLECTION",
			// Select ST_GeomFromText("GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))")
			Hex:  "0000000001070000000300000001010000000000000000002440000000000000244001010000000000000000003E400000000000003E400102000000020000000000000000002E400000000000002E4000000000000034400000000000003440",
			Json: "{\"type\":\"GeometryCollection\",\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}]}",
		},
	}

	for _, geoTest := range tests {
		t.Run(fmt.Sprintf("geometry_type_%v", geoTest.Type), func(t *testing.T) {
			geometry, err := hex.DecodeString(geoTest.Hex)
			if err != nil {
				t.Fatalf("failed to encode geometry value with %q", err)
			}
			geometryTypeTest(t, geometry, geoTest.Json)
		})
	}
}

func geometryTypeTest(t *testing.T, geometry []byte, geojson string) {
	geometryTypeResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Type_GEOMETRY", Type: querypb.Type_GEOMETRY},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_GEOMETRY, geometry),
			},
		},
	}

	ctx := context.Background()
	intValue := strconv.AppendInt(nil, int64(int8(12)), 10)
	_, mysqlClientConstructor := setupUpdateRowsTest(intValue)

	clientConstructor := func() lib.ConnectClient {
		return &lib.TestConnectClient{
			ListShardsFn: func(ctx context.Context, ps lib.PlanetScaleSource) ([]string, error) {
				return []string{"-", "-40"}, nil
			},
			CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
				return nil
			},
			ReadFn: func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
				tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
			) (*lib.SerializedCursor, error) {
				assert.Equal(t, "customers", tableName)
				assert.NotNil(t, columns)
				if err := onResult(geometryTypeResult, lib.OpType_Insert); err != nil {
					return nil, status.Errorf(codes.Internal, "unable to serialize row : %s", err.Error())
				}
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

	for _, f := range geometryTypeResult.Fields {
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
	require.NoError(t, err)
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
	assert.Len(t, rows, 5)
	operation := rows[1].GetOperation()
	require.NotNil(t, operation)
	record, ok := operation.(*fivetransdk.UpdateResponse_Record)
	assert.True(t, ok)
	assert.NotNil(t, record)
	assert.Equal(t, "SalesDB", *record.Record.SchemaName)
	assert.Equal(t, "customers", record.Record.TableName)

	assert.Equal(t, record.Record.Type, fivetransdk.RecordType_UPSERT)

	assert.Equal(t, "UPSERT", record.Record.Type.String())
	value := record.Record.Data["Type_GEOMETRY"]
	assert.NotNil(t, value)
	jsonValue := value.Inner.(*fivetransdk.ValueType_Json)
	assert.NotNil(t, jsonValue)
	assert.Equal(t, geojson, jsonValue.Json)
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
			ReadFn: func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
				tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
			) (*lib.SerializedCursor, error) {
				assert.Equal(t, "customers", tableName)
				assert.NotNil(t, columns)
				if err := onResult(allTypesResult, lib.OpType_Insert); err != nil {
					return nil, status.Errorf(codes.Internal, "unable to serialize row : %s", err.Error())
				}
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
	require.NoError(t, err)
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
	assert.Len(t, rows, 7)
	operation := rows[3].GetOperation()
	require.NotNil(t, operation)
	record, ok := operation.(*fivetransdk.UpdateResponse_Record)
	assert.True(t, ok)
	assert.NotNil(t, record)
	assert.Equal(t, "SalesDB", *record.Record.SchemaName)
	assert.Equal(t, "customers", record.Record.TableName)

	assert.Equal(t, record.Record.Type, fivetransdk.RecordType_UPSERT)
	for _, field := range allTypesResult.Fields {
		assert.NotEmpty(t, record.Record.Data[field.Name].Inner, "expected value for %q field", field.Name)
		assert.NotNil(t, record.Record.Data[field.Name].Inner, "expected value for %q field", field.Name)
	}

	operation = rows[len(rows)-1].GetOperation()
	checkpoint, ok := operation.(*fivetransdk.UpdateResponse_Checkpoint)
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

func TestUpdateReturnsErrors(t *testing.T) {
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
			ReadFn: func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
				tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
			) (*lib.SerializedCursor, error) {
				assert.Equal(t, "customers", tableName)
				assert.NotNil(t, columns)

				return nil, fmt.Errorf("unable to serialize: %v", "DataType.BIG_INT")
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
	var tErr error
	for {
		resp, err := out.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			tErr = err
			break
		}
		rows = append(rows, resp)
	}
	assert.Len(t, rows, 1)
	assert.NotNil(t, tErr)
	assert.Equal(t, "rpc error: code = Internal desc = failed to download rows for table : customers , error : unable to serialize: DataType.BIG_INT", tErr.Error())
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
			ReadFn: func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
				tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
			) (*lib.SerializedCursor, error) {
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
	assert.Len(t, rows, 7)
	operation := rows[3].GetOperation()
	assert.NotNil(t, operation)
	record, ok := operation.(*fivetransdk.UpdateResponse_Record)
	assert.True(t, ok)
	assert.NotNil(t, record)
	assert.Equal(t, "SalesDB", *record.Record.SchemaName)
	assert.Equal(t, "customers", record.Record.TableName)

	assert.Equal(t, record.Record.Type, fivetransdk.RecordType_DELETE)
	assert.Equal(t, 1, len(record.Record.Data))

	assert.Equal(t, &fivetransdk.ValueType_Int{Int: 12}, record.Record.Data["Type_INT8"].Inner)

	operation = rows[len(rows)-1].GetOperation()
	checkpoint, ok := operation.(*fivetransdk.UpdateResponse_Checkpoint)
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

func TestUpdateReturnsUpdates(t *testing.T) {
	ctx := context.Background()
	beforeIntValue := strconv.AppendInt(nil, int64(int8(12)), 10)
	beforeResult, _ := setupUpdateRowsTest(beforeIntValue)
	afterIntValue := strconv.AppendInt(nil, int64(int8(17)), 10)
	afterResult, mysqlClientConstructor := setupUpdateRowsTest(afterIntValue)

	clientConstructor := func() lib.ConnectClient {
		return &lib.TestConnectClient{
			ListShardsFn: func(ctx context.Context, ps lib.PlanetScaleSource) ([]string, error) {
				return []string{"-", "-40"}, nil
			},
			CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
				return nil
			},
			ReadFn: func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string, tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate) (*lib.SerializedCursor, error) {
				assert.Equal(t, "customers", tableName)
				assert.NotNil(t, columns)
				onUpdate(&lib.UpdatedRow{
					Before: beforeResult,
					After:  afterResult,
				})
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

	for _, f := range beforeResult.Fields {
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
	assert.Len(t, rows, 7)
	operation := rows[3].GetOperation()
	assert.NotNil(t, operation)
	record, ok := operation.(*fivetransdk.UpdateResponse_Record)
	assert.True(t, ok)
	assert.NotNil(t, record)
	assert.Equal(t, "SalesDB", *record.Record.SchemaName)
	assert.Equal(t, "customers", record.Record.TableName)

	assert.Equal(t, record.Record.Type, fivetransdk.RecordType_UPDATE)
	assert.Equal(t, 10, len(record.Record.Data))

	assert.Equal(t, &fivetransdk.ValueType_Int{Int: 17}, record.Record.Data["Type_INT8"].Inner)
	assert.Equal(t, &fivetransdk.ValueType_Int{Int: 17}, record.Record.Data["Type_UINT8"].Inner)
	assert.Equal(t, &fivetransdk.ValueType_Int{Int: 17}, record.Record.Data["Type_INT16"].Inner)
	assert.Equal(t, &fivetransdk.ValueType_Int{Int: 17}, record.Record.Data["Type_UINT16"].Inner)
	assert.Equal(t, &fivetransdk.ValueType_Int{Int: 17}, record.Record.Data["Type_INT24"].Inner)
	assert.Equal(t, &fivetransdk.ValueType_Int{Int: 17}, record.Record.Data["Type_INT32"].Inner)
	assert.Equal(t, &fivetransdk.ValueType_String_{String_: "17"}, record.Record.Data["Type_UINT32"].Inner)
	assert.Equal(t, &fivetransdk.ValueType_Long{Long: 17}, record.Record.Data["Type_INT64"].Inner)
	assert.Equal(t, &fivetransdk.ValueType_Long{Long: 17}, record.Record.Data["Type_INT64"].Inner)

	operation = rows[len(rows)-1].GetOperation()
	checkpoint, ok := operation.(*fivetransdk.UpdateResponse_Checkpoint)
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
	geometry, err := hex.DecodeString("0000000001010000003E0A325D63345CC0761FDB8D99D94840")
	if err != nil {
		panic(err)
	}
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
				sqltypes.MakeTrusted(querypb.Type_GEOMETRY, geometry),
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
						{Name: "Type_INT8", Type: "int", IsPrimaryKey: true},
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
						{Name: "Type_ENUM", Type: "enum('cat','dog','bird')"},
						{Name: "Type_SET", Type: "set('cat','dog','bird','hamster')"},
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

			ReadFn: func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
				tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
			) (*lib.SerializedCursor, error) {
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
	assert.Len(t, rows, 3)
	operation := rows[1].GetOperation()
	assert.NotNil(t, operation)
	checkpoint, ok := operation.(*fivetransdk.UpdateResponse_Checkpoint)
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
