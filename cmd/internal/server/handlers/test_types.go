package handlers

import (
	"context"
	"io"

	"github.com/pkg/errors"

	"vitess.io/vitess/go/sqltypes"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-proto/go"
	"google.golang.org/grpc"
)

type testLogSender struct {
	sendError    error
	lastResponse *fivetransdk.UpdateResponse
}

func (l *testLogSender) Send(response *fivetransdk.UpdateResponse) error {
	l.lastResponse = response
	return l.sendError
}

type testLogger struct{}

func (testLogger) Log(level fivetransdk.LogLevel, s string) error {
	// TODO implement me
	panic("implement me")
}

func (testLogger) Record(result *sqltypes.Result, selection *fivetransdk.SchemaSelection, selection2 *fivetransdk.TableSelection) error {
	// TODO implement me
	panic("implement me")
}

func (testLogger) State(state SyncState) error {
	return nil
}

func (testLogger) Release() {
	// TODO implement me
	panic("implement me")
}

type dbLogMessage struct {
	level   fivetransdk.LogLevel
	message string
}
type dbLogger struct {
	messages []dbLogMessage
}

func (dbl *dbLogger) Log(level fivetransdk.LogLevel, s string) error {
	dbl.messages = append(dbl.messages, dbLogMessage{
		level:   level,
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
	grpc.ClientStream
}

func (x *connectSyncClientMock) Recv() (*psdbconnect.SyncResponse, error) {
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

type (
	ReadFunc             func(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, table *fivetransdk.TableSelection, tc *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor) (*SerializedCursor, error)
	DiscoverFunc         func(ctx context.Context, ps PlanetScaleSource) (*fivetransdk.SchemaResponse, error)
	CanConnectFunc       func(ctx context.Context, ps PlanetScaleSource) error
	ListVitessShardsFunc func(ctx context.Context, ps PlanetScaleSource) ([]string, error)

	TestConnectClient struct {
		ReadFn             ReadFunc
		DiscoverFn         DiscoverFunc
		CanConnectFn       CanConnectFunc
		ListVitessShardsFn ListVitessShardsFunc
	}
)

func blankDiscoverFn(ctx context.Context, ps PlanetScaleSource) (*fivetransdk.SchemaResponse, error) {
	return nil, nil
}

func (tcc *TestConnectClient) ListShards(ctx context.Context, ps PlanetScaleSource) ([]string, error) {
	if tcc.ListVitessShardsFn != nil {
		return tcc.ListVitessShardsFn(ctx, ps)
	}

	panic("implement me")
}

func (tcc *TestConnectClient) CanConnect(ctx context.Context, ps PlanetScaleSource) error {
	if tcc.CanConnectFn != nil {
		return tcc.CanConnectFn(ctx, ps)
	}
	return errors.New("Unimplemented")
}

func (tcc *TestConnectClient) Read(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, table *fivetransdk.TableSelection, tc *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor) (*SerializedCursor, error) {
	if tcc.ReadFn != nil {
		return tcc.ReadFn(ctx, logger, ps, table, tc, onResult, onCursor)
	}

	return nil, errors.New("Unimplemented")
}

func (tcc *TestConnectClient) DiscoverSchema(ctx context.Context, ps PlanetScaleSource) (*fivetransdk.SchemaResponse, error) {
	if tcc.DiscoverFn != nil {
		return tcc.DiscoverFn(ctx, ps)
	}

	return nil, nil
}

func NewTestConnectClient(r ReadFunc, d DiscoverFunc) PlanetScaleDatabase {
	return &TestConnectClient{ReadFn: r, DiscoverFn: d}
}

type statefulRequest struct {
	stateJson string
}

func (sfr statefulRequest) GetStateJson() string {
	return sfr.stateJson
}
