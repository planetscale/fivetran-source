package lib

import (
	"context"
	"io"

	"github.com/pkg/errors"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"google.golang.org/grpc"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
)

type dbLogMessage struct {
	message string
}
type dbLogger struct {
	messages          []dbLogMessage
	binlogExpirations []string
}

func (dbl *dbLogger) RecordBinlogExpiration(keyspace, table, shard string) {
	dbl.binlogExpirations = append(dbl.binlogExpirations, keyspace+"."+table+"/"+shard)
}

func (dbl *dbLogger) Info(s string) error {
	dbl.messages = append(dbl.messages, dbLogMessage{
		message: s,
	})

	return nil
}

func (dbl *dbLogger) Warning(s string) error {
	dbl.messages = append(dbl.messages, dbLogMessage{
		message: s,
	})

	return nil
}

type clientConnectionMock struct {
	syncFn             func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error)
	syncFnInvoked      bool
	syncFnInvokedCount int
}

type connectSyncClientMock struct {
	lastResponseSent int
	syncResponses    []*psdbconnect.SyncResponse
	recvFn           func() (*psdbconnect.SyncResponse, error)
	grpc.ClientStream
}

func (x *connectSyncClientMock) Recv() (*psdbconnect.SyncResponse, error) {
	if x.recvFn != nil {
		return x.recvFn()
	}
	if x.lastResponseSent >= len(x.syncResponses) {
		return nil, io.EOF
	}
	x.lastResponseSent += 1
	return x.syncResponses[x.lastResponseSent-1], nil
}

func (c *clientConnectionMock) Sync(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
	c.syncFnInvoked = true
	c.syncFnInvokedCount += 1
	return c.syncFn(ctx, in, opts...)
}

type vstreamConnectionMock struct {
	vstreamFn             func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error)
	vstreamFnInvoked      bool
	vstreamFnInvokedCount int
}

func (c *vstreamConnectionMock) VStream(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
	c.vstreamFnInvoked = true
	c.vstreamFnInvokedCount += 1
	return c.vstreamFn(ctx, in, opts...)
}

type vstreamClientMock struct {
	lastResponseSent int
	responses        []*vtgatepb.VStreamResponse
	recvFn           func() (*vtgatepb.VStreamResponse, error)
	grpc.ClientStream
}

func (x *vstreamClientMock) Recv() (*vtgatepb.VStreamResponse, error) {
	if x.recvFn != nil {
		return x.recvFn()
	}
	if x.lastResponseSent >= len(x.responses) {
		return nil, io.EOF
	}
	x.lastResponseSent += 1
	return x.responses[x.lastResponseSent-1], nil
}

type (
	BuildSchemaFunc              func(ctx context.Context, psc PlanetScaleSource, schemaBuilder SchemaBuilder) error
	PingContextFunc              func(context.Context, PlanetScaleSource) error
	GetVitessShardsFunc          func(ctx context.Context, psc PlanetScaleSource) ([]string, error)
	GetKeyspaceTableColumnsFunc  func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error)
	GetShardTableColumnOrderFunc func(ctx context.Context, keyspaceName, shard, tableName string, tabletType psdbconnect.TabletType) ([]string, error)
	GetShardTableColumnsFunc     func(ctx context.Context, keyspaceName, shard, tableName string, tabletType psdbconnect.TabletType) ([]ColumnDetail, error)
	TestMysqlClient              struct {
		BuildSchemaFn              BuildSchemaFunc
		PingContextFn              PingContextFunc
		GetVitessShardsFn          GetVitessShardsFunc
		GetKeyspaceTableColumnsFn  GetKeyspaceTableColumnsFunc
		GetShardTableColumnOrderFn GetShardTableColumnOrderFunc
		GetShardTableColumnsFn     GetShardTableColumnsFunc
	}
)

func (t TestMysqlClient) GetKeyspaceTableColumns(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
	if t.GetKeyspaceTableColumnsFn != nil {
		return t.GetKeyspaceTableColumnsFn(ctx, keyspaceName, tableName)
	}

	panic("GetKeyspaceTableColumnsFunc is not implemented")
}

func (t TestMysqlClient) GetShardTableColumnOrder(ctx context.Context, keyspaceName, shard, tableName string, tabletType psdbconnect.TabletType) ([]string, error) {
	if t.GetShardTableColumnOrderFn != nil {
		return t.GetShardTableColumnOrderFn(ctx, keyspaceName, shard, tableName, tabletType)
	}
	panic("GetShardTableColumnOrder is not implemented")
}

func (t TestMysqlClient) GetShardTableColumns(ctx context.Context, keyspaceName, shard, tableName string, tabletType psdbconnect.TabletType) ([]ColumnDetail, error) {
	if t.GetShardTableColumnsFn != nil {
		return t.GetShardTableColumnsFn(ctx, keyspaceName, shard, tableName, tabletType)
	}
	panic("GetShardTableColumns is not implemented")
}

func (t TestMysqlClient) BuildSchema(ctx context.Context, psc PlanetScaleSource, schemaBuilder SchemaBuilder) error {
	if t.BuildSchemaFn != nil {
		return t.BuildSchemaFn(ctx, psc, schemaBuilder)
	}

	panic("BuildSchema is not implemented")
}

func (t TestMysqlClient) PingContext(ctx context.Context, source PlanetScaleSource) error {
	if t.PingContextFn != nil {
		return t.PingContextFn(ctx, source)
	}

	panic("PingContext is not implemented")
}

func (t TestMysqlClient) GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	if t.GetVitessShardsFn != nil {
		return t.GetVitessShardsFn(ctx, psc)
	}
	panic("GetVitessShards is not implemented")
}

func (t TestMysqlClient) Close() error {
	return nil
}

func NewTestMysqlClient(gktc GetKeyspaceTableColumnsFunc) MysqlClient {
	return &TestMysqlClient{
		GetKeyspaceTableColumnsFn: gktc,
	}
}

type (
	ReadFunc               func(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, columns []string, tc *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor, onUpdate OnUpdate) (*SerializedCursor, error)
	CanConnectFunc         func(ctx context.Context, ps PlanetScaleSource) error
	ListShardsFunc         func(ctx context.Context, ps PlanetScaleSource) ([]string, error)
	ScanSchemaFunc         func(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, tc *psdbconnect.TableCursor, resume *SchemaScanProgress) (SchemaHistory, error)
	ColumnOrderFunc        func(ctx context.Context, keyspace, tableName string) ([]string, error)
	ShardColumnOrderFunc   func(ctx context.Context, ps PlanetScaleSource, keyspace, shard, tableName string) ([]string, error)
	ShardColumnDetailsFunc func(ctx context.Context, ps PlanetScaleSource, keyspace, shard, tableName string) ([]ColumnDetail, error)

	TestConnectClient struct {
		ReadFn               ReadFunc
		CanConnectFn         CanConnectFunc
		ListShardsFn         ListShardsFunc
		ScanSchemaHistoryFn  ScanSchemaFunc
		ColumnOrderFn        ColumnOrderFunc
		ShardColumnOrderFn   ShardColumnOrderFunc
		ShardColumnDetailsFn ShardColumnDetailsFunc
	}
)

func (tcc *TestConnectClient) ListShards(ctx context.Context, ps PlanetScaleSource) ([]string, error) {
	if tcc.ListShardsFn != nil {
		return tcc.ListShardsFn(ctx, ps)
	}

	panic("implement me")
}

func (tcc *TestConnectClient) CanConnect(ctx context.Context, ps PlanetScaleSource) error {
	if tcc.CanConnectFn != nil {
		return tcc.CanConnectFn(ctx, ps)
	}
	return errors.New("CanConnect is Unimplemented")
}

func (tcc *TestConnectClient) Read(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, columns []string, lastKnownPosition *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor, onUpdate OnUpdate) (*SerializedCursor, error) {
	if tcc.ReadFn != nil {
		return tcc.ReadFn(ctx, logger, ps, tableName, columns, lastKnownPosition, onResult, onCursor, onUpdate)
	}

	return nil, errors.New("Read is Unimplemented")
}

// ScanSchemaHistory implements SchemaHistoryScanner; without
// ScanSchemaHistoryFn it reports ErrSchemaHistoryUnavailable.
func (tcc *TestConnectClient) ScanSchemaHistory(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, tc *psdbconnect.TableCursor, resume *SchemaScanProgress) (SchemaHistory, error) {
	if tcc.ScanSchemaHistoryFn != nil {
		return tcc.ScanSchemaHistoryFn(ctx, logger, ps, tableName, tc, resume)
	}
	return SchemaHistory{}, ErrSchemaHistoryUnavailable
}

// ColumnOrder implements ColumnOrderReader; without ColumnOrderFn it reports
// ErrColumnOrderUnavailable (the handler then uses the source schema's order).
func (tcc *TestConnectClient) ColumnOrder(ctx context.Context, keyspace, tableName string) ([]string, error) {
	if tcc.ColumnOrderFn != nil {
		return tcc.ColumnOrderFn(ctx, keyspace, tableName)
	}
	return nil, ErrColumnOrderUnavailable
}

// ShardColumnOrder implements ShardColumnOrderReader; without
// ShardColumnOrderFn it fails, so the handler treats every shard's live order
// as unknown (like a shard target that does not work).
func (tcc *TestConnectClient) ShardColumnOrder(ctx context.Context, ps PlanetScaleSource, keyspace, shard, tableName string) ([]string, error) {
	if tcc.ShardColumnOrderFn != nil {
		return tcc.ShardColumnOrderFn(ctx, ps, keyspace, shard, tableName)
	}
	return nil, ErrColumnOrderUnavailable
}

// ShardColumnDetails implements ShardColumnDetailsReader; without
// ShardColumnDetailsFn it reports ErrColumnOrderUnavailable (no step-over).
func (tcc *TestConnectClient) ShardColumnDetails(ctx context.Context, ps PlanetScaleSource, keyspace, shard, tableName string) ([]ColumnDetail, error) {
	if tcc.ShardColumnDetailsFn != nil {
		return tcc.ShardColumnDetailsFn(ctx, ps, keyspace, shard, tableName)
	}
	return nil, ErrColumnOrderUnavailable
}

func NewTestConnectClient(r ReadFunc) ConnectClient {
	return &TestConnectClient{ReadFn: r}
}
