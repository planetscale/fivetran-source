package lib

import (
	"context"
	"io"

	"github.com/pkg/errors"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"google.golang.org/grpc"
)

type dbLogMessage struct {
	message string
}
type dbLogger struct {
	messages []dbLogMessage
}

func (dbl *dbLogger) Info(s string) {
	dbl.messages = append(dbl.messages, dbLogMessage{
		message: s,
	})
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
	ReadFunc             func(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, tc *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor) (*SerializedCursor, error)
	CanConnectFunc       func(ctx context.Context, ps PlanetScaleSource) error
	ListVitessShardsFunc func(ctx context.Context, ps PlanetScaleSource) ([]string, error)

	TestConnectClient struct {
		ReadFn             ReadFunc
		CanConnectFn       CanConnectFunc
		ListVitessShardsFn ListVitessShardsFunc
	}
)

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

func (tcc *TestConnectClient) Read(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, lastKnownPosition *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor) (*SerializedCursor, error) {
	if tcc.ReadFn != nil {
		return tcc.ReadFn(ctx, logger, ps, tableName, lastKnownPosition, onResult, onCursor)
	}

	return nil, errors.New("Unimplemented")
}

func NewTestConnectClient(r ReadFunc) ConnectClient {
	return &TestConnectClient{ReadFn: r}
}
