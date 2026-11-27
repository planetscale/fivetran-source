package lib

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	vtschema "vitess.io/vitess/go/vt/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
)

func TestRead_CanPeekBeforeRead(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}, {Name: "email", Type: "varchar(256)", IsPrimaryKey: false}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{
				Cursor: tc,
			},
			{
				Cursor: tc,
			},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, "current", in.Cursor.Position)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{}
	onRow := func(*sqltypes.Result, Operation) error {
		return nil
	}
	onCursor := func(*psdbconnect.TableCursor) error {
		return nil
	}
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", nil, tc, onRow, onCursor, nil)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_CanEarlyExitIfNoNewVGtidInPeek(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}, {Name: "email", Type: "varchar(256)", IsPrimaryKey: false}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: tc},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, "current", in.Cursor.Position)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{}
	onRow := func(*sqltypes.Result, Operation) error {
		return nil
	}
	onCursor := func(*psdbconnect.TableCursor) error {
		return nil
	}
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", nil, tc, onRow, onCursor, nil)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc, "should return original cursor if no new rows found")
	assert.Equal(t, 1, cc.syncFnInvokedCount)
	assert.Contains(t, dbl.messages[len(dbl.messages)-1].message, "no new rows found, exiting")
}

func TestRead_ReturnsLatestCursorSyncError(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}, {Name: "email", Type: "varchar(256)", IsPrimaryKey: false}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, "current", in.Cursor.Position)
			return nil, errors.New("sync unavailable")
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{}, "customers", nil, tc, nil, nil, nil)
	assert.Nil(t, sc)
	assert.ErrorContains(t, err, "Unable to get latest cursor position")
	assert.ErrorContains(t, err, "sync unavailable")
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_ReturnsLatestCursorRecvError(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}, {Name: "email", Type: "varchar(256)", IsPrimaryKey: false}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	getCurrentVGtidClient := &connectSyncClientMock{}
	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, "current", in.Cursor.Position)
			return getCurrentVGtidClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{}, "customers", nil, tc, nil, nil, nil)
	assert.Nil(t, sc)
	assert.ErrorContains(t, err, "Unable to get latest cursor position")
	assert.ErrorContains(t, err, "EOF")
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_CanPickPrimaryForShardedKeyspaces(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}, {Name: "email", Type: "varchar(256)", IsPrimaryKey: false}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient
	tc := &psdbconnect.TableCursor{
		Shard:    "40-80",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: tc},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			assert.Contains(t, in.Cells, "planetscale_operator_default")
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	onRow := func(*sqltypes.Result, Operation) error {
		return nil
	}
	onCursor := func(*psdbconnect.TableCursor) error {
		return nil
	}
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", nil, tc, onRow, onCursor, nil)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_CanPickReplicaForShardedKeyspaces(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}, {Name: "email", Type: "varchar(256)", IsPrimaryKey: false}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient
	tc := &psdbconnect.TableCursor{
		Shard:    "40-80",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: tc},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_replica, in.TabletType)
			assert.Contains(t, in.Cells, "planetscale_operator_default")
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database:   "connect-test",
		UseReplica: true,
	}
	onRow := func(*sqltypes.Result, Operation) error {
		return nil
	}
	onCursor := func(*psdbconnect.TableCursor) error {
		return nil
	}
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", nil, tc, onRow, onCursor, nil)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_CanReturnNewCursorIfNewFound(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}
	newTC := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "I_AM_FARTHER_IN_THE_BINLOG",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: newTC},
			{Cursor: newTC},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	onRow := func(*sqltypes.Result, Operation) error {
		return nil
	}
	onCursor := func(*psdbconnect.TableCursor) error {
		return nil
	}

	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}, {Name: "email", Type: "varchar(256)", IsPrimaryKey: false}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient

	sc, err := ped.Read(context.Background(), dbl, ps, "customers", nil, tc, onRow, onCursor, nil)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(newTC)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 2, cc.syncFnInvokedCount)
}

func TestRead_ReturnsVStreamSchemaIncompatibilityErrors(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}
	stopCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "I_AM_THE_CURRENT_BINLOG_POSITION",
		Keyspace: "connect-test",
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			if in.Cursor.Position == "current" {
				return &connectSyncClientMock{
					syncResponses: []*psdbconnect.SyncResponse{
						{Cursor: stopCursor},
					},
				}, nil
			}
			return nil, status.Error(codes.Unknown, vstreamColumnNotFoundErrorMessage)
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{
			{Name: "id", Type: "bigint", IsPrimaryKey: true},
			{Name: "before_col", Type: "varchar(64)", IsPrimaryKey: false},
			{Name: "after_col", Type: "varchar(64)", IsPrimaryKey: false},
		}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id", "before_col", "after_col"}, tc, nil, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "historical re-sync")
	assert.True(t, IsVStreamSchemaIncompatibilityError(err))
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	// peek, stream, give up: without WithAddedColumnRetry the column is not left out.
	assert.Equal(t, 2, cc.syncFnInvokedCount)

	// With it: peek, stream, peek, stream without after_col (still not found), give up.
	cc.syncFnInvokedCount = 0
	_, err = ped.Read(WithAddedColumnRetry(context.Background()), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id", "before_col", "after_col"}, tc, nil, nil, nil)
	assert.True(t, IsVStreamSchemaIncompatibilityError(err))
	assert.Equal(t, 4, cc.syncFnInvokedCount)
}

func TestRead_ReturnsGenericNonTimeoutErrors(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}
	stopCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "I_AM_THE_CURRENT_BINLOG_POSITION",
		Keyspace: "connect-test",
	}

	getCurrentVGtidClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: stopCursor},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			if in.Cursor.Position == "current" {
				return getCurrentVGtidClient, nil
			}
			return nil, status.Error(codes.Unavailable, "tablet unavailable")
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id"}, tc, nil, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tablet unavailable")
	assert.NotContains(t, err.Error(), "historical re-sync")
	assert.False(t, IsVStreamSchemaIncompatibilityError(err))
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 2, cc.syncFnInvokedCount)
}

func TestRead_CallbackErrorsReturnStartCursor(t *testing.T) {
	testFields := sqltypes.MakeTestFields("pid|description", "int64|varbinary")

	tests := []struct {
		name         string
		response     *psdbconnect.SyncResponse
		onRow        OnResult
		onUpdate     OnUpdate
		errorMessage string
	}{
		{
			name: "insert callback",
			response: &psdbconnect.SyncResponse{
				Result: []*query.QueryResult{
					sqltypes.ResultToProto3(sqltypes.MakeTestResult(testFields, "12|new_monitor")),
				},
			},
			onRow: func(_ *sqltypes.Result, op Operation) error {
				if op == OpType_Insert {
					return errors.New("serialize failed")
				}
				return nil
			},
			errorMessage: "unable to serialize row",
		},
		{
			name: "delete callback",
			response: &psdbconnect.SyncResponse{
				Deletes: []*psdbconnect.DeletedRow{
					{
						Result: sqltypes.ResultToProto3(sqltypes.MakeTestResult(testFields, "12|deleted_monitor")),
					},
				},
			},
			onRow: func(_ *sqltypes.Result, op Operation) error {
				if op == OpType_Delete {
					return errors.New("serialize failed")
				}
				return nil
			},
			errorMessage: "unable to serialize row",
		},
		{
			name: "update callback",
			response: &psdbconnect.SyncResponse{
				Updates: []*psdbconnect.UpdatedRow{
					{
						Before: sqltypes.ResultToProto3(sqltypes.MakeTestResult(testFields, "12|old_monitor")),
						After:  sqltypes.ResultToProto3(sqltypes.MakeTestResult(testFields, "12|new_monitor")),
					},
				},
			},
			onUpdate: func(*UpdatedRow) error {
				return errors.New("serialize failed")
			},
			errorMessage: "unable to serialize update",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbl := &dbLogger{}
			ped := connectClient{}
			tc := &psdbconnect.TableCursor{
				Shard:    "-",
				Position: "THIS_IS_A_SHARD_GTID",
				Keyspace: "connect-test",
			}
			newTC := &psdbconnect.TableCursor{
				Shard:    "-",
				Position: "I_AM_FARTHER_IN_THE_BINLOG",
				Keyspace: "connect-test",
			}

			getCurrentVGtidClient := &connectSyncClientMock{
				syncResponses: []*psdbconnect.SyncResponse{{Cursor: newTC}},
			}
			syncClient := &connectSyncClientMock{
				syncResponses: []*psdbconnect.SyncResponse{
					{Cursor: newTC},
					tt.response,
				},
			}

			cc := clientConnectionMock{
				syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
					if in.Cursor.Position == "current" {
						return getCurrentVGtidClient, nil
					}
					return syncClient, nil
				},
			}
			ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
				return &cc, nil
			}
			getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
				return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}}, nil
			}
			mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
			ped.Mysql = &mysqlClient

			sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", nil, tc, tt.onRow, nil, tt.onUpdate)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorMessage)
			esc, err := TableCursorToSerializedCursor(tc)
			assert.NoError(t, err)
			assert.Equal(t, esc, sc)
		})
	}
}

func TestRead_CanStopAtWellKnownCursor(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}

	testFields := sqltypes.MakeTestFields(
		"pid|description",
		"int64|varbinary",
	)
	numResponses := 10
	// when the client tries to get the "current" vgtid,
	// we return the ante-penultimate element of the array.
	currentVGtidPosition := (numResponses * 3) - 4
	// this is the next vgtid that should stop the sync session.
	nextVGtidPosition := currentVGtidPosition + 1
	responses := make([]*psdbconnect.SyncResponse, 0, numResponses)
	for i := 0; i < numResponses; i++ {
		// this simulates multiple events being returned, for the same vgtid, from vstream
		for x := 0; x < 3; x++ {
			var (
				inserts []*query.QueryResult
				deletes []*psdbconnect.DeletedRow
			)
			if x == 2 {
				inserts = []*query.QueryResult{
					sqltypes.ResultToProto3(sqltypes.MakeTestResult(
						testFields,
						fmt.Sprintf("%v|keyboard", i+1),
						fmt.Sprintf("%v|monitor", i+2),
					)),
				}
				deletes = []*psdbconnect.DeletedRow{
					{
						Result: sqltypes.ResultToProto3(sqltypes.MakeTestResult(
							testFields,
							fmt.Sprintf("%v|deleted_monitor", i+12),
						)),
					},
					{
						Result: sqltypes.ResultToProto3(sqltypes.MakeTestResult(
							testFields,
							fmt.Sprintf("%v|deleted_monitor", i+12),
						)),
					},
				}
			}

			vgtid := fmt.Sprintf("e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", i)
			responses = append(responses, &psdbconnect.SyncResponse{
				Cursor: &psdbconnect.TableCursor{
					Shard:    "-",
					Keyspace: "connect-test",
					Position: vgtid,
				},
				Result:  inserts,
				Deletes: deletes,
			})
		}
	}

	syncClient := &connectSyncClientMock{
		syncResponses: responses,
	}

	getCurrentVGtidClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			responses[currentVGtidPosition],
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			if in.Cursor.Position == "current" {
				return getCurrentVGtidClient, nil
			}

			return syncClient, nil
		},
	}

	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	insertedRowCounter := 0
	deletedRowCounter := 0
	onRow := func(res *sqltypes.Result, op Operation) error {
		if op == OpType_Insert {
			insertedRowCounter += 1
		}
		if op == OpType_Delete {
			deletedRowCounter += 1
		}
		return nil
	}
	onCursor := func(*psdbconnect.TableCursor) error {
		return nil
	}

	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}, {Name: "email", Type: "varchar(256)", IsPrimaryKey: false}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient

	sc, err := ped.Read(context.Background(), dbl, ps, "customers", nil, responses[0].Cursor, onRow, onCursor, nil)

	assert.NoError(t, err)
	// sync should start at the first vgtid
	esc, err := TableCursorToSerializedCursor(responses[nextVGtidPosition].Cursor)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 2, cc.syncFnInvokedCount)

	assert.Equal(t, "[connect-test:customers shard:- tabletType:primary] Finished reading all rows for table [customers]", dbl.messages[len(dbl.messages)-1].message)
	assert.Equal(t, 2*(nextVGtidPosition/3), insertedRowCounter)
	assert.Equal(t, 2*(nextVGtidPosition/3), deletedRowCounter)
}

func TestRead_FiltersNonExistentColumns(t *testing.T) {
	tests := []struct {
		name             string
		tableColumns     []MysqlColumn
		requestedColumns []string
		expectedColumns  []string
		err              error
	}{
		{
			name: "filters nonexistent columns",
			tableColumns: []MysqlColumn{
				{Name: "id", Type: "bigint", IsPrimaryKey: true},
				{Name: "email", Type: "varchar(256)", IsPrimaryKey: false},
				{Name: "name", Type: "varchar(256)", IsPrimaryKey: false},
			},
			requestedColumns: []string{"id", "email", "nonexistent_column"},
			expectedColumns:  []string{"id", "email"},
		},
		{
			name:             "uses requested columns on error",
			tableColumns:     nil,
			requestedColumns: []string{"id", "email", "nonexistent_column"},
			expectedColumns:  []string{"id", "email", "nonexistent_column"},
			err:              errors.New("error fetching columns"),
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbl := &dbLogger{}
			ped := connectClient{}

			getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
				return tt.tableColumns, tt.err
			}

			newTC := &psdbconnect.TableCursor{
				Shard:    "-",
				Position: "I_AM_FARTHER_IN_THE_BINLOG",
				Keyspace: "connect-test",
			}

			tc := &psdbconnect.TableCursor{
				Shard:    "-",
				Position: "THIS_IS_A_SHARD_GTID",
				Keyspace: "connect-test",
			}

			syncClient := &connectSyncClientMock{
				syncResponses: []*psdbconnect.SyncResponse{
					{
						Cursor: newTC,
					},
					{
						Cursor: newTC,
					},
				},
			}

			var firstExpectedColumns []string
			run := 1

			syncFn := func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
				if run == 1 {
					assert.Equal(t, firstExpectedColumns, in.Columns)
				} else {
					assert.Equal(t, tt.expectedColumns, in.Columns)
				}
				run += 1
				return syncClient, nil
			}

			mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
			ped.Mysql = &mysqlClient

			cc := clientConnectionMock{
				syncFn: syncFn,
			}
			ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
				return &cc, nil
			}
			ps := PlanetScaleSource{}
			onRow := func(*sqltypes.Result, Operation) error {
				return nil
			}
			onCursor := func(*psdbconnect.TableCursor) error {
				return nil
			}
			sc, err := ped.Read(ctx, dbl, ps, "customers", tt.requestedColumns, tc, onRow, onCursor, nil)
			assert.NoError(t, err)
			esc, err := TableCursorToSerializedCursor(newTC)
			assert.NoError(t, err)
			assert.Equal(t, esc, sc)
			assert.Equal(t, 2, cc.syncFnInvokedCount)
		})
	}
}

func TestRead_ReturnsLastKnownPKCursorAfterMaxNoProgressTimeout(t *testing.T) {
	originalMaxTimeouts := maxConsecutiveSyncTimeouts
	maxConsecutiveSyncTimeouts = 1
	t.Cleanup(func() {
		maxConsecutiveSyncTimeouts = originalMaxTimeouts
	})

	dbl := &dbLogger{}
	ped := connectClient{}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient

	stopCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "STOP_GTID",
	}
	copyCursor := &psdbconnect.TableCursor{
		Shard:       "-",
		Keyspace:    "connect-test",
		LastKnownPk: testLastKnownPK("42"),
	}

	getCurrentVGtidClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{{Cursor: stopCursor}},
	}
	syncClient := &connectSyncClientMock{
		recvFn: func() (*psdbconnect.SyncResponse, error) {
			return nil, status.Error(codes.DeadlineExceeded, "server deadline")
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			if in.Cursor.Position == "current" {
				return getCurrentVGtidClient, nil
			}
			assert.Empty(t, in.Cursor.Position)
			assert.NotNil(t, in.Cursor.LastKnownPk)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id"}, copyCursor, nil, nil, nil)
	assert.ErrorIs(t, err, ErrReadStoppedEarly)
	if assert.NotNil(t, sc) {
		cursor, err := sc.SerializedCursorToTableCursor()
		assert.NoError(t, err)
		assert.Empty(t, cursor.Position)
		assert.NotNil(t, cursor.LastKnownPk)
	}
	assert.Equal(t, 2, cc.syncFnInvokedCount)
}

func TestRead_DoesNotStopAfterProgressingTimeoutWindows(t *testing.T) {
	originalMaxTimeouts := maxConsecutiveSyncTimeouts
	maxConsecutiveSyncTimeouts = 3
	t.Cleanup(func() {
		maxConsecutiveSyncTimeouts = originalMaxTimeouts
	})

	dbl := &dbLogger{}
	ped := connectClient{}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient

	initialCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
	}
	stopCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "STOP_GTID",
	}
	afterStopCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "AFTER_STOP_GTID",
	}
	testFields := sqltypes.MakeTestFields("id|name", "int64|varbinary")
	progressingWindows := maxConsecutiveSyncTimeouts + 2
	syncAttempts := 0

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			if in.Cursor.Position == "current" {
				return &connectSyncClientMock{
					syncResponses: []*psdbconnect.SyncResponse{{Cursor: stopCursor}},
				}, nil
			}

			syncAttempts++
			if syncAttempts <= progressingWindows {
				lastPK := fmt.Sprintf("%d", syncAttempts)
				sentResponse := false
				return &connectSyncClientMock{
					recvFn: func() (*psdbconnect.SyncResponse, error) {
						if !sentResponse {
							sentResponse = true
							return &psdbconnect.SyncResponse{
								Result: []*query.QueryResult{
									sqltypes.ResultToProto3(sqltypes.MakeTestResult(testFields, fmt.Sprintf("%s|copied", lastPK))),
								},
								Cursor: &psdbconnect.TableCursor{
									Shard:       "-",
									Keyspace:    "connect-test",
									LastKnownPk: testLastKnownPK(lastPK),
								},
							}, nil
						}
						return nil, status.Error(codes.DeadlineExceeded, "server deadline")
					},
				}, nil
			}

			return &connectSyncClientMock{
				syncResponses: []*psdbconnect.SyncResponse{
					{Cursor: stopCursor},
					{Cursor: afterStopCursor},
				},
			}, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}

	rows := 0
	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id"}, initialCursor, func(*sqltypes.Result, Operation) error {
		rows++
		return nil
	}, nil, nil)
	assert.NoError(t, err)
	if assert.NotNil(t, sc) {
		cursor, err := sc.SerializedCursorToTableCursor()
		assert.NoError(t, err)
		assert.Equal(t, afterStopCursor.Position, cursor.Position)
		assert.Nil(t, cursor.LastKnownPk)
	}
	assert.Equal(t, progressingWindows, rows)
	assert.Equal(t, progressingWindows+1, syncAttempts)
	for _, msg := range dbl.messages {
		assert.NotContains(t, msg.message, "Reached maximum consecutive no-progress timeouts")
		assert.NotContains(t, msg.message, "Stopping sync.")
	}
}

func TestRead_CancelDuringTimeoutBackoffReturnsImmediately(t *testing.T) {
	originalMaxTimeouts := maxConsecutiveSyncTimeouts
	maxConsecutiveSyncTimeouts = 2
	t.Cleanup(func() {
		maxConsecutiveSyncTimeouts = originalMaxTimeouts
	})

	dbl := &dbLogger{}
	ped := connectClient{}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient

	startCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "START_GTID",
	}
	stopCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "STOP_GTID",
	}

	ctx, cancel := context.WithCancel(context.Background())
	getCurrentVGtidClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{{Cursor: stopCursor}},
	}
	syncClient := &connectSyncClientMock{
		recvFn: func() (*psdbconnect.SyncResponse, error) {
			cancel()
			return nil, status.Error(codes.DeadlineExceeded, "server deadline")
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			if in.Cursor.Position == "current" {
				return getCurrentVGtidClient, nil
			}
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}

	start := time.Now()
	_, err := ped.Read(ctx, dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id"}, startCursor, nil, nil, nil)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), time.Second)
}

func TestRead_ResumesHistoricalCopyEvenWhenPeekMatchesCopyCursorPosition(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient

	copyCursor := &psdbconnect.TableCursor{
		Shard:       "-",
		Keyspace:    "connect-test",
		Position:    "COPY_SNAPSHOT_GTID",
		LastKnownPk: testLastKnownPK("42"),
	}
	stopCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "COPY_SNAPSHOT_GTID",
	}
	doneCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "AFTER_COPY_GTID",
	}

	getCurrentVGtidClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{{Cursor: stopCursor}},
	}
	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: stopCursor},
			{Cursor: doneCursor},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			if in.Cursor.Position == "current" {
				return getCurrentVGtidClient, nil
			}
			assert.Equal(t, "COPY_SNAPSHOT_GTID", in.Cursor.Position)
			assert.NotNil(t, in.Cursor.LastKnownPk)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id"}, copyCursor, nil, nil, nil)
	assert.NoError(t, err)
	if assert.NotNil(t, sc) {
		cursor, err := sc.SerializedCursorToTableCursor()
		assert.NoError(t, err)
		assert.Equal(t, "AFTER_COPY_GTID", cursor.Position)
		assert.Nil(t, cursor.LastKnownPk)
	}
	assert.Equal(t, 2, cc.syncFnInvokedCount)
}

func TestRead_BinlogExpirationReturnsResetCursor(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	getKeyspaceTableColumnsFunc := func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}}, nil
	}
	mysqlClient := NewTestMysqlClient(getKeyspaceTableColumnsFunc)
	ped.Mysql = &mysqlClient

	initialCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "OLD_GTID",
	}
	stopCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "STOP_GTID",
	}
	copyCursor := &psdbconnect.TableCursor{
		Shard:       "-",
		Keyspace:    "connect-test",
		LastKnownPk: testLastKnownPK("42"),
	}
	copyResponseSent := false
	currentCursorRequests := 0

	syncClient := &connectSyncClientMock{
		recvFn: func() (*psdbconnect.SyncResponse, error) {
			if !copyResponseSent {
				copyResponseSent = true
				return &psdbconnect.SyncResponse{Cursor: copyCursor}, nil
			}
			return nil, status.Error(codes.Unknown, "Cannot replicate because the source purged required binary logs")
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			if in.Cursor.Position == "current" {
				currentCursorRequests++
				if currentCursorRequests == 1 {
					return &connectSyncClientMock{
						syncResponses: []*psdbconnect.SyncResponse{{Cursor: stopCursor}},
					}, nil
				}
				return nil, errors.New("peek failed after reset")
			}
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id"}, initialCursor, nil, nil, nil)
	assert.ErrorContains(t, err, "peek failed after reset")
	if assert.NotNil(t, sc) {
		cursor, err := sc.SerializedCursorToTableCursor()
		assert.NoError(t, err)
		assert.Empty(t, cursor.Position)
		assert.Nil(t, cursor.LastKnownPk)
		if assert.NotNil(t, sc.ErrorCode) {
			assert.Equal(t, "BINLOG_EXPIRATION_ERROR", *sc.ErrorCode)
		}
		if assert.NotNil(t, sc.ErrorMessage) {
			assert.Contains(t, *sc.ErrorMessage, "Binlogs have expired")
		}
	}
	assert.Equal(t, 3, cc.syncFnInvokedCount)
	assert.Equal(t, []string{"connect-test.customers/-"}, dbl.binlogExpirations)
}

func TestSync_DirectVStreamHandlesRawEventsAndCopyCompleted(t *testing.T) {
	originalCheckpointRows := cursorCheckpointRows
	originalCheckpointInterval := cursorCheckpointInterval
	cursorCheckpointRows = 1
	cursorCheckpointInterval = time.Hour
	t.Cleanup(func() {
		cursorCheckpointRows = originalCheckpointRows
		cursorCheckpointInterval = originalCheckpointInterval
	})

	dbl := &dbLogger{}
	ped := connectClient{}
	testFields := sqltypes.MakeTestFields("id|name", "int64|varbinary")
	rows := sqltypes.ResultToProto3(sqltypes.MakeTestResult(testFields, "1|first", "2|second")).Rows
	lastPK := testLastKnownPK("2")
	startCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
	}
	copyCursor := &psdbconnect.TableCursor{
		Shard:       "-",
		Keyspace:    "connect-test",
		Position:    "COPY_GTID",
		LastKnownPk: lastPK,
	}
	doneCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "AFTER_COPY_GTID",
	}

	rawStream := &vstreamClientMock{
		responses: []*vtgatepb.VStreamResponse{
			{
				Events: []*binlogdatapb.VEvent{
					{
						Type: binlogdatapb.VEventType_FIELD,
						FieldEvent: &binlogdatapb.FieldEvent{
							TableName: "connect-test.customers",
							Fields:    testFields,
						},
					},
					{
						Type: binlogdatapb.VEventType_ROW,
						RowEvent: &binlogdatapb.RowEvent{
							TableName: "connect-test.customers",
							RowChanges: []*binlogdatapb.RowChange{
								{After: rows[0]},
								{After: rows[1]},
							},
						},
					},
					vstreamVGtidEventFromCursor("connect-test.customers", copyCursor),
				},
			},
			{
				Events: []*binlogdatapb.VEvent{
					{Type: binlogdatapb.VEventType_COPY_COMPLETED},
					vstreamVGtidEventFromCursor("connect-test.customers", doneCursor),
				},
			},
		},
	}
	vc := &vstreamConnectionMock{
		vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
			assert.Equal(t, topodatapb.TabletType_PRIMARY, in.TabletType)
			assert.Equal(t, "connect-test", in.Vgtid.ShardGtids[0].Keyspace)
			assert.Equal(t, "-", in.Vgtid.ShardGtids[0].Shard)
			assert.Empty(t, in.Vgtid.ShardGtids[0].Gtid)
			assert.Equal(t, "customers", in.Filter.Rules[0].Match)
			assert.Equal(t, "SELECT `id`,`name` FROM `customers`", in.Filter.Rules[0].Filter)
			assert.Equal(t, "planetscale_operator_default", in.Flags.Cells)
			assert.True(t, in.Flags.MinimizeSkew)
			return rawStream, nil
		},
	}
	ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) {
		return vc, nil
	}

	records := 0
	checkpoints := []*psdbconnect.TableCursor{}
	returnedCursor, err := ped.sync(context.Background(), dbl, "customers", []string{"id", "name"}, false, startCursor, "STOP_GTID", PlanetScaleSource{Database: "connect-test"}, psdbconnect.TabletType_primary, time.Second, func(*sqltypes.Result, Operation) error {
		records++
		return nil
	}, func(cursor *psdbconnect.TableCursor) error {
		checkpoints = append(checkpoints, cloneTableCursor(cursor))
		return nil
	}, nil)

	assert.True(t, errors.Is(err, io.EOF))
	assert.True(t, proto.Equal(doneCursor, returnedCursor))
	assert.Equal(t, 2, records)
	assert.Equal(t, 1, vc.vstreamFnInvokedCount)
	if assert.Len(t, checkpoints, 2) {
		assert.True(t, proto.Equal(copyCursor, checkpoints[0]))
		assert.True(t, proto.Equal(doneCursor, checkpoints[1]))
	}
}

func TestSync_DirectVStreamRejectsFieldlessLastKnownPK(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	lastPK := testLastKnownPK("2")
	lastPK.Fields = nil
	startCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
	}
	copyCursor := &psdbconnect.TableCursor{
		Shard:       "-",
		Keyspace:    "connect-test",
		Position:    "COPY_GTID",
		LastKnownPk: lastPK,
	}

	rawStream := &vstreamClientMock{
		responses: []*vtgatepb.VStreamResponse{
			{
				Events: []*binlogdatapb.VEvent{
					vstreamVGtidEventFromCursor("customers", copyCursor),
				},
			},
		},
	}
	vc := &vstreamConnectionMock{
		vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
			return rawStream, nil
		},
	}
	ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) {
		return vc, nil
	}

	returnedCursor, err := ped.sync(context.Background(), dbl, "customers", []string{"id", "name"}, false, startCursor, "STOP_GTID", PlanetScaleSource{Database: "connect-test"}, psdbconnect.TabletType_primary, time.Second, nil, nil, nil)

	assert.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.ErrorContains(t, err, "missing LastKnownPk field metadata")
	assert.True(t, proto.Equal(startCursor, returnedCursor))
	assert.Equal(t, 1, vc.vstreamFnInvokedCount)
}

func TestSync_CheckpointsHistoricalCopyProgress(t *testing.T) {
	originalCheckpointRows := cursorCheckpointRows
	originalCheckpointInterval := cursorCheckpointInterval
	cursorCheckpointRows = 1
	cursorCheckpointInterval = time.Hour
	t.Cleanup(func() {
		cursorCheckpointRows = originalCheckpointRows
		cursorCheckpointInterval = originalCheckpointInterval
	})

	dbl := &dbLogger{}
	ped := connectClient{}
	testFields := sqltypes.MakeTestFields("id|name", "int64|varbinary")
	startCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
	}
	copyCursor := &psdbconnect.TableCursor{
		Shard:       "-",
		Keyspace:    "connect-test",
		LastKnownPk: testLastKnownPK("2"),
	}
	stopCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "STOP_GTID",
	}
	afterStopCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "AFTER_STOP_GTID",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{
				Result: []*query.QueryResult{
					sqltypes.ResultToProto3(sqltypes.MakeTestResult(testFields, "1|first", "2|second")),
				},
				Cursor: copyCursor,
			},
			{Cursor: stopCursor},
			{Cursor: afterStopCursor},
		},
	}
	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Empty(t, in.Cursor.Position)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}

	rows := 0
	checkpoints := []*psdbconnect.TableCursor{}
	onResult := func(*sqltypes.Result, Operation) error {
		rows++
		return nil
	}
	onCursor := func(cursor *psdbconnect.TableCursor) error {
		checkpoints = append(checkpoints, cloneTableCursor(cursor))
		return nil
	}

	returnedCursor, err := ped.sync(context.Background(), dbl, "customers", []string{"id", "name"}, false, startCursor, stopCursor.Position, PlanetScaleSource{Database: "connect-test"}, psdbconnect.TabletType_primary, time.Second, onResult, onCursor, nil)
	assert.True(t, errors.Is(err, io.EOF))
	assert.True(t, proto.Equal(afterStopCursor, returnedCursor))
	assert.Equal(t, 2, rows)
	if assert.Len(t, checkpoints, 2) {
		assert.True(t, proto.Equal(copyCursor, checkpoints[0]))
		assert.True(t, proto.Equal(afterStopCursor, checkpoints[1]))
	}
}

func TestSync_DoesNotPeriodicallyCheckpointVGTIDProgress(t *testing.T) {
	originalCheckpointRows := cursorCheckpointRows
	originalCheckpointInterval := cursorCheckpointInterval
	cursorCheckpointRows = 1
	cursorCheckpointInterval = time.Hour
	t.Cleanup(func() {
		cursorCheckpointRows = originalCheckpointRows
		cursorCheckpointInterval = originalCheckpointInterval
	})

	dbl := &dbLogger{}
	ped := connectClient{}
	testFields := sqltypes.MakeTestFields("id|name", "int64|varbinary")
	startCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "START_GTID",
	}
	vgtidCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "MID_GTID",
	}
	stopCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "STOP_GTID",
	}
	afterStopCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "AFTER_STOP_GTID",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{
				Result: []*query.QueryResult{
					sqltypes.ResultToProto3(sqltypes.MakeTestResult(testFields, "1|first")),
				},
				Cursor: vgtidCursor,
			},
			{Cursor: stopCursor},
			{Cursor: afterStopCursor},
		},
	}
	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}

	checkpoints := []*psdbconnect.TableCursor{}
	onCursor := func(cursor *psdbconnect.TableCursor) error {
		checkpoints = append(checkpoints, cloneTableCursor(cursor))
		return nil
	}

	returnedCursor, err := ped.sync(context.Background(), dbl, "customers", []string{"id", "name"}, false, startCursor, stopCursor.Position, PlanetScaleSource{Database: "connect-test"}, psdbconnect.TabletType_primary, time.Second, func(*sqltypes.Result, Operation) error {
		return nil
	}, onCursor, nil)
	assert.True(t, errors.Is(err, io.EOF))
	assert.True(t, proto.Equal(afterStopCursor, returnedCursor))
	if assert.Len(t, checkpoints, 1) {
		assert.True(t, proto.Equal(afterStopCursor, checkpoints[0]))
	}
}

func TestSync_ResumesHistoricalCopyWithLastKnownPKOnly(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	resumeCursor := &psdbconnect.TableCursor{
		Shard:       "-",
		Keyspace:    "connect-test",
		LastKnownPk: testLastKnownPK("42"),
	}

	requestChecked := false
	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			requestChecked = true
			assert.Empty(t, in.Cursor.Position)
			assert.NotNil(t, in.Cursor.LastKnownPk)
			return &connectSyncClientMock{}, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}

	_, err := ped.sync(context.Background(), dbl, "customers", []string{"id"}, false, resumeCursor, "STOP_GTID", PlanetScaleSource{Database: "connect-test"}, psdbconnect.TabletType_primary, time.Second, nil, nil, nil)
	assert.True(t, errors.Is(err, io.EOF))
	assert.True(t, requestChecked)
}

func TestSync_ResumesHistoricalCopyWithPositionAndLastKnownPK(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	resumeCursor := &psdbconnect.TableCursor{
		Shard:       "-",
		Keyspace:    "connect-test",
		Position:    "COPY_SNAPSHOT_GTID",
		LastKnownPk: testLastKnownPK("42"),
	}

	requestChecked := false
	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			requestChecked = true
			assert.Equal(t, "COPY_SNAPSHOT_GTID", in.Cursor.Position)
			assert.NotNil(t, in.Cursor.LastKnownPk)
			return &connectSyncClientMock{}, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}

	_, err := ped.sync(context.Background(), dbl, "customers", []string{"id"}, false, resumeCursor, "STOP_GTID", PlanetScaleSource{Database: "connect-test"}, psdbconnect.TabletType_primary, time.Second, nil, nil, nil)
	assert.True(t, errors.Is(err, io.EOF))
	assert.True(t, requestChecked)
}

func TestIsVStreamSchemaIncompatibilityError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "column not found while building table replication plan",
			err:  status.Error(codes.Unknown, vstreamColumnNotFoundErrorMessage),
			want: true,
		},
		{
			name: "synthetic table map column names",
			err: status.Error(codes.Unknown, "stream error: Code: FAILED_PRECONDITION\n"+
				"cannot use column names in vstream filter as the current table schema for table customers is not compatible with the current event for this table in the stream\n\n"+
				"failed to build table replication plan for table customers"),
			want: true,
		},
		{
			name: "column not found rewrapped by vtgate after retries",
			err: status.Error(codes.Unknown, `error starting stream from shard GTID keyspace:"ks" shard:"-" gtid:"MySQL56/abc:1-10": `+
				`persistent error in vstream for ks/- on tablet zone1-0000000101; giving up: target: ks.-.replica: vttablet: `+
				`rpc error: code = FailedPrecondition desc = stream (at source tablet) error @ (including the GTID we failed to process) MySQL56/abc:1-11: `+
				`failed to build table replication plan for table customers: column after_col not found in table customers`),
			want: true,
		},
		{
			name: "binlog expiration is not schema incompatibility",
			err:  errors.New("Cannot replicate because the source purged required binary logs"),
			want: false,
		},
		{
			name: "generic failed precondition is not enough",
			err:  status.Error(codes.Unknown, "Code: FAILED_PRECONDITION\nanother replication error"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsVStreamSchemaIncompatibilityError(tt.err))
		})
	}
}

func testLastKnownPK(value string) *query.QueryResult {
	return &query.QueryResult{
		Fields: []*query.Field{
			{
				Type: sqltypes.Int64,
				Name: "id",
			},
		},
		Rows: []*query.Row{
			{
				Lengths: []int64{int64(len(value))},
				Values:  []byte(value),
			},
		},
	}
}

const vstreamColumnNotFoundErrorMessage = "error starting stream from shard GTID keyspace:\"fivetran\" shard:\"-\": persistent error in vstream: " +
	"stream (at source tablet) error @ (including the GTID we failed to process): Code: FAILED_PRECONDITION\n" +
	"column after_col not found in table customers\n\n" +
	"failed to build table replication plan for table customers\n" +
	"failed to parse transaction payload's internal event"

const vstreamReplicaNotFoundErrorMessage = `error starting stream from shard GTID keyspace:"connect-test" shard:"-" gtid:"MySQL56/abc:1-10": ` +
	`failed to find a REPLICA tablet for VStream in connect-test/- within the planetscale_operator_default cell(s): context has expired`

func TestIsVStreamTabletNotFoundError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		tabletType psdbconnect.TabletType
		want       bool
	}{
		{"replica picker gave up", status.Error(codes.Canceled, vstreamReplicaNotFoundErrorMessage), psdbconnect.TabletType_replica, true},
		{
			"primary picker gave up",
			status.Error(codes.Canceled, `failed to find a PRIMARY tablet for VStream in ks/-80 within the planetscale_operator_default cell(s): context has expired`),
			psdbconnect.TabletType_primary, true,
		},
		{"different tablet type than requested", status.Error(codes.Canceled, vstreamReplicaNotFoundErrorMessage), psdbconnect.TabletType_primary, false},
		{
			"picker error that is not a timeout",
			status.Error(codes.Unknown, `failed to find a REPLICA tablet for VStream in ks/- within the zone1 cell(s): no tablets available`),
			psdbconnect.TabletType_replica, false,
		},
		{"unrelated error", errors.New("connection reset by peer"), psdbconnect.TabletType_replica, false},
		{"nil", nil, psdbconnect.TabletType_replica, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsVStreamTabletNotFoundError(tt.err, tt.tabletType))
		})
	}
}

func TestIsVStreamSchemaIncompatibilityError_ColumnCountMismatch(t *testing.T) {
	err := status.Error(codes.Unknown, "vttablet: rpc error: code = Unknown desc = stream (at source tablet) error @ (including the GTID we failed to process) abc:1-10: "+
		"cannot determine table columns for user: event has [15 3 15], schema has [name:\"id\" type:INT64]")
	assert.True(t, IsVStreamSchemaIncompatibilityError(err))
}

// readWithPeekAndSync runs Read with a peek ("current") handler and a sync handler.
func readWithPeekAndSync(t *testing.T, ps PlanetScaleSource, peek, sync func(in *psdbconnect.SyncRequest) (psdbconnect.Connect_SyncClient, error)) (*SerializedCursor, *clientConnectionMock, error) {
	t.Helper()
	ped := connectClient{}
	mysqlClient := NewTestMysqlClient(func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}}, nil
	})
	ped.Mysql = &mysqlClient
	cc := &clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			if in.Cursor.Position == "current" {
				return peek(in)
			}
			return sync(in)
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return cc, nil
	}
	tc := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "OLD_GTID"}
	sc, err := ped.Read(context.Background(), &dbLogger{}, ps, "customers", []string{"id"}, tc, nil, nil, nil)
	return sc, cc, err
}

func peekReturns(position string) func(*psdbconnect.SyncRequest) (psdbconnect.Connect_SyncClient, error) {
	return func(*psdbconnect.SyncRequest) (psdbconnect.Connect_SyncClient, error) {
		return &connectSyncClientMock{syncResponses: []*psdbconnect.SyncResponse{{
			Cursor: &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: position},
		}}}, nil
	}
}

func TestRead_TabletNotFoundAfterSuccessfulPeekIsSuspectedBinlogExpiration(t *testing.T) {
	sc, cc, err := readWithPeekAndSync(t, PlanetScaleSource{Database: "connect-test", UseReplica: true},
		peekReturns("NEW_GTID"),
		func(*psdbconnect.SyncRequest) (psdbconnect.Connect_SyncClient, error) {
			return &connectSyncClientMock{recvFn: func() (*psdbconnect.SyncResponse, error) {
				return nil, status.Error(codes.Canceled, vstreamReplicaNotFoundErrorMessage)
			}}, nil
		})
	var suspected *SuspectedBinlogExpirationError
	if assert.True(t, errors.As(err, &suspected), "got %v", err) {
		assert.Equal(t, "connect-test", suspected.Keyspace)
		assert.Equal(t, "-", suspected.Shard)
		assert.Equal(t, "customers", suspected.Table)
	}
	// The masked form never takes the explicit-expiry reset path (#69/#73): no
	// restart from an empty position and no BINLOG_EXPIRATION_ERROR marker.
	assert.False(t, IsBinlogsExpirationError(err))
	assert.Equal(t, 2, cc.syncFnInvokedCount, "one position check and one sync")
	if sc != nil {
		assert.Nil(t, sc.ErrorCode)
		assert.Nil(t, sc.ErrorMessage)
	}
}

func TestRead_TabletNotFoundDuringPeekIsNotSuspectedBinlogExpiration(t *testing.T) {
	_, _, err := readWithPeekAndSync(t, PlanetScaleSource{Database: "connect-test", UseReplica: true},
		func(*psdbconnect.SyncRequest) (psdbconnect.Connect_SyncClient, error) {
			return nil, status.Error(codes.Canceled, vstreamReplicaNotFoundErrorMessage)
		},
		func(*psdbconnect.SyncRequest) (psdbconnect.Connect_SyncClient, error) {
			t.Fatal("sync must not run when the peek fails")
			return nil, nil
		})
	assert.ErrorContains(t, err, "Unable to get latest cursor position")
	var suspected *SuspectedBinlogExpirationError
	assert.False(t, errors.As(err, &suspected))
}

type testGRPCError struct{ code codes.Code }

func (e *testGRPCError) Error() string              { return "typed failure" }
func (e *testGRPCError) GRPCStatus() *status.Status { return status.New(e.code, e.Error()) }

func TestRowCallbackErrorKeepsCause(t *testing.T) {
	typed := &testGRPCError{code: codes.Internal}
	err := rowCallbackError("unable to serialize row", typed)
	var got *testGRPCError
	assert.True(t, errors.As(err, &got))
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "unable to serialize row: typed failure")

	// A failed Send to Fivetran keeps its own (retryable) status code.
	sendErr := status.Error(codes.Unavailable, "transport is closing")
	assert.Equal(t, codes.Unavailable, status.Code(rowCallbackError("unable to serialize row", sendErr)))

	// Plain errors stay codes.Internal, as before, but now include the cause.
	plain := rowCallbackError("unable to serialize row", errors.New("boom"))
	assert.Equal(t, codes.Internal, status.Code(plain))
	assert.Contains(t, plain.Error(), "unable to serialize row: boom")
}

func TestVStreamMissingFilterColumn(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		column string
		ok     bool
	}{
		{
			name:   "vttablet error",
			err:    status.Error(codes.Unknown, vstreamColumnNotFoundErrorMessage),
			column: "after_col",
			ok:     true,
		},
		{
			name: "rewrapped by vtgate after retries",
			err: status.Error(codes.FailedPrecondition, `error starting stream from shard GTID keyspace:"ks" shard:"-" gtid:"MySQL56/abc:1-10": `+
				`persistent error in vstream for ks/- on tablet zone1-0000000101; giving up: target: ks.-.primary: vttablet: `+
				`rpc error: code = FailedPrecondition desc = stream (at source tablet) error @ (including the GTID we failed to process) MySQL56/abc:1-11: `+
				`failed to build table replication plan for table orders: column added_col not found in table orders`),
			column: "added_col",
			ok:     true,
		},
		{
			name:   "quoted column name",
			err:    errors.New("failed to build table replication plan for table t1: column `select` not found in table t1"),
			column: "select",
			ok:     true,
		},
		{
			name: "positional fields are not a missing column",
			err: errors.New("failed to build table replication plan for table t1: cannot use column names in vstream filter as the current " +
				"table schema for table t1 is not compatible with the current event for this table in the stream"),
		},
		{
			name: "dropped column is not a missing column",
			err:  errors.New("failed to build table replication plan for table t1: cannot determine table columns for t1: event has [8 15 15], schema has [id]"),
		},
		{
			name: "unrelated error",
			err:  errors.New("column x not found in table t1"),
		},
		{name: "nil"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			column, ok := VStreamMissingFilterColumn(tt.err)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.column, column)
		})
	}
}

// A column added by an Online DDL after the saved position: the first stream
// fails on t's pre-cut-over events, the retry leaves the column out and stops
// at the cut-over RENAME, and the stream then restarts there with every column.
func TestRead_StepsOverColumnAddedAfterCursor(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	// Names as Vitess v0.24 Online DDL generates them (schema.GenerateInternalTableName
	// with the "vrp" hint, and schema.GenerateGCTableName for the "hld" sentry table).
	shadowTable := "_vt_vrp_0a1b2c3d4e5f60718293a4b5c6d7e8f9_20261123210400_"
	sentryTable := "_vt_hld_f9e8d7c6b5a4938271605f4e3d2c1b0a_20261126210557_"
	assert.True(t, vtschema.IsInternalOperationTableName(shadowTable))
	assert.True(t, vtschema.IsInternalOperationTableName(sentryTable))
	cutOver := fmt.Sprintf("RENAME TABLE `customers` TO `%s`, `%s` TO `customers`, `%s` TO `%s`", sentryTable, shadowTable, sentryTable, shadowTable)
	cursorAt := func(position string) *psdbconnect.TableCursor {
		return &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: position}
	}
	startCursor := cursorAt("BEFORE_DDL")
	ddlCursor := cursorAt("CUT_OVER")
	stopCursor := cursorAt("STOP")
	nextCursor := cursorAt("AFTER_STOP")

	oldFields := sqltypes.MakeTestFields("id|name", "int64|varbinary")
	newFields := sqltypes.MakeTestFields("id|name|added_col", "int64|varbinary|varbinary")
	oldRows := sqltypes.ResultToProto3(sqltypes.MakeTestResult(oldFields, "1|first")).Rows
	newRows := sqltypes.ResultToProto3(sqltypes.MakeTestResult(newFields, "2|second|new")).Rows

	missingColumnErr := status.Error(codes.FailedPrecondition, `persistent error in vstream for connect-test/- on tablet zone1-0000000101; giving up: `+
		`vttablet: rpc error: code = FailedPrecondition desc = failed to build table replication plan for table customers: `+
		`column added_col not found in table customers`)

	var filters, startPositions []string
	vc := &vstreamConnectionMock{
		vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
			gtid := in.Vgtid.ShardGtids[0].Gtid
			if gtid == "current" {
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{{
					Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("customers", stopCursor)},
				}}}, nil
			}
			filters = append(filters, in.Filter.Rules[0].Filter)
			startPositions = append(startPositions, gtid)
			switch len(filters) {
			case 1:
				return &vstreamClientMock{recvFn: func() (*vtgatepb.VStreamResponse, error) { return nil, missingColumnErr }}, nil
			case 2:
				// What vtgate forwards for an Online DDL (vitess strategy) between
				// the saved position and the cut-over, with the reduced filter:
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
					// An application write to t, still in the old layout.
					{Events: []*binlogdatapb.VEvent{
						{Type: binlogdatapb.VEventType_BEGIN},
						{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "connect-test.customers", Fields: oldFields}},
						{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "connect-test.customers", RowChanges: []*binlogdatapb.RowChange{{After: oldRows[0]}}}},
						vstreamVGtidEventFromCursor("customers", cursorAt("APP_WRITE")),
						{Type: binlogdatapb.VEventType_COMMIT},
					}},
					// CREATE and ALTER of the shadow table, and the sentry table: they
					// don't name t, so vttablet sends OTHER instead of DDL.
					{Events: []*binlogdatapb.VEvent{
						vstreamVGtidEventFromCursor("customers", cursorAt("SHADOW_CREATE")),
						{Type: binlogdatapb.VEventType_OTHER},
						vstreamVGtidEventFromCursor("customers", cursorAt("SHADOW_ALTER")),
						{Type: binlogdatapb.VEventType_OTHER},
					}},
					// VReplication backfill into the shadow table. vttablet never sends
					// these (internal table, and no rule matches); if one got through,
					// it is for another table and must be ignored.
					{Events: []*binlogdatapb.VEvent{
						{Type: binlogdatapb.VEventType_BEGIN},
						{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "connect-test." + shadowTable, RowChanges: []*binlogdatapb.RowChange{{After: newRows[0]}}}},
						vstreamVGtidEventFromCursor("customers", cursorAt("BACKFILL")),
						{Type: binlogdatapb.VEventType_COMMIT},
					}},
					{Events: []*binlogdatapb.VEvent{
						vstreamVGtidEventFromCursor("customers", cursorAt("SENTRY_CREATE")),
						{Type: binlogdatapb.VEventType_OTHER},
					}},
					// The cut-over: the only statement that names t.
					{Events: []*binlogdatapb.VEvent{
						vstreamVGtidEventFromCursor("customers", ddlCursor),
						{Type: binlogdatapb.VEventType_DDL, Statement: cutOver},
					}},
					// Must not be read: writes to the new t, in the new layout.
					{Events: []*binlogdatapb.VEvent{
						{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "connect-test.customers", RowChanges: []*binlogdatapb.RowChange{{After: newRows[0]}}}},
					}},
				}}, nil
			default:
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
					{Events: []*binlogdatapb.VEvent{
						{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "connect-test.customers", Fields: newFields}},
						{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "connect-test.customers", RowChanges: []*binlogdatapb.RowChange{{After: newRows[0]}}}},
						vstreamVGtidEventFromCursor("customers", stopCursor),
					}},
					{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("customers", nextCursor)}},
				}}, nil
			}
		},
	}
	ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) {
		return vc, nil
	}
	mysqlClient := NewTestMysqlClient(func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{
			{Name: "id", Type: "bigint", IsPrimaryKey: true},
			{Name: "name", Type: "varchar(64)"},
			{Name: "added_col", Type: "varchar(64)"},
		}, nil
	})
	ped.Mysql = &mysqlClient

	var rows []*sqltypes.Result
	var checkpoints []string
	sc, err := ped.Read(WithAddedColumnRetry(context.Background()), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id", "name", "added_col"}, startCursor,
		func(res *sqltypes.Result, _ Operation) error {
			rows = append(rows, res)
			return nil
		},
		func(cursor *psdbconnect.TableCursor) error {
			checkpoints = append(checkpoints, cursor.Position)
			return nil
		}, nil)

	assert.NoError(t, err)
	assert.Equal(t, []string{
		"SELECT `id`,`name`,`added_col` FROM `customers`",
		"SELECT `id`,`name` FROM `customers`",
		"SELECT `id`,`name`,`added_col` FROM `customers`",
	}, filters)
	assert.Equal(t, []string{"BEFORE_DDL", "BEFORE_DDL", "CUT_OVER"}, startPositions)
	if assert.Len(t, rows, 2) {
		assert.Equal(t, []string{"id", "name"}, fieldNames(rows[0].Fields))
		assert.Equal(t, []string{"id", "name", "added_col"}, fieldNames(rows[1].Fields))
	}
	assert.Contains(t, checkpoints, "CUT_OVER")
	tc, err := sc.SerializedCursorToTableCursor()
	assert.NoError(t, err)
	assert.Equal(t, "AFTER_STOP", tc.Position)
}

// A missing column that the filter does not ask for (or one that is still
// missing after it was left out) is not retried: it keeps raising the
// re-sync error.
func TestRead_MissingColumnNotInFilterIsNotRetried(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	startCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "BEFORE_DDL"}
	stopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "STOP"}
	streams := 0
	vc := &vstreamConnectionMock{
		vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
			if in.Vgtid.ShardGtids[0].Gtid == "current" {
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{{
					Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("customers", stopCursor)},
				}}}, nil
			}
			streams++
			return &vstreamClientMock{recvFn: func() (*vtgatepb.VStreamResponse, error) {
				return nil, status.Error(codes.FailedPrecondition, "vttablet: rpc error: code = FailedPrecondition desc = "+
					"failed to build table replication plan for table customers: column other_col not found in table customers")
			}}, nil
		},
	}
	ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) {
		return vc, nil
	}
	mysqlClient := NewTestMysqlClient(func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}, {Name: "name", Type: "varchar(64)"}}, nil
	})
	ped.Mysql = &mysqlClient

	_, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id", "name"}, startCursor, nil, nil, nil)
	assert.Error(t, err)
	assert.True(t, IsVStreamSchemaIncompatibilityError(err))
	assert.Equal(t, 1, streams)
}

func TestHandleVStreamRows_PositionalFieldsAreSchemaIncompatibility(t *testing.T) {
	fields := sqltypes.MakeTestFields("@1|@2", "int64|varbinary")
	rows := sqltypes.ResultToProto3(sqltypes.MakeTestResult(fields, "1|first")).Rows
	fieldsByTable := map[string][]*query.Field{}
	cacheVStreamFields(fieldsByTable, "customers", fields)
	called := false
	_, err := handleVStreamRows("customers", &binlogdatapb.RowEvent{
		TableName:  "customers",
		RowChanges: []*binlogdatapb.RowChange{{After: rows[0]}},
	}, fieldsByTable, func(*sqltypes.Result, Operation) error {
		called = true
		return nil
	}, nil)
	assert.Error(t, err)
	assert.True(t, IsVStreamSchemaIncompatibilityError(err))
	assert.False(t, called)
}

func fieldNames(fields []*query.Field) []string {
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.Name)
	}
	return names
}

// With every column in the filter the stream doesn't stop at a cut-over.
func TestSync_RenameDoesNotStopFullColumnStream(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	fields := sqltypes.MakeTestFields("id|name", "int64|varbinary")
	rows := sqltypes.ResultToProto3(sqltypes.MakeTestResult(fields, "1|first")).Rows
	cursorAt := func(position string) *psdbconnect.TableCursor {
		return &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: position}
	}
	vc := &vstreamConnectionMock{
		vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
			return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
				{Events: []*binlogdatapb.VEvent{
					vstreamVGtidEventFromCursor("customers", cursorAt("CUT_OVER")),
					{Type: binlogdatapb.VEventType_DDL, Statement: "RENAME TABLE `customers` TO `_vt_hld_f9e8d7c6b5a4938271605f4e3d2c1b0a_20261126210557_`, " +
						"`_vt_vrp_0a1b2c3d4e5f60718293a4b5c6d7e8f9_20261123210400_` TO `customers`, " +
						"`_vt_hld_f9e8d7c6b5a4938271605f4e3d2c1b0a_20261126210557_` TO `_vt_vrp_0a1b2c3d4e5f60718293a4b5c6d7e8f9_20261123210400_`"},
				}},
				{Events: []*binlogdatapb.VEvent{
					{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "connect-test.customers", Fields: fields}},
					{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "connect-test.customers", RowChanges: []*binlogdatapb.RowChange{{After: rows[0]}}}},
					vstreamVGtidEventFromCursor("customers", cursorAt("STOP")),
				}},
				{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("customers", cursorAt("AFTER_STOP"))}},
			}}, nil
		},
	}
	ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) {
		return vc, nil
	}
	records := 0
	tc, err := ped.sync(context.Background(), dbl, "customers", []string{"id", "name"}, false, cursorAt("BEFORE_DDL"), "STOP", PlanetScaleSource{Database: "connect-test"},
		psdbconnect.TabletType_primary, time.Second, func(*sqltypes.Result, Operation) error {
			records++
			return nil
		}, nil, nil)
	assert.True(t, errors.Is(err, io.EOF))
	assert.Equal(t, 1, records)
	assert.Equal(t, "AFTER_STOP", tc.Position)
}

// An incompatible column type change (e2e case type_incompatible) makes Read
// return the schema-incompatibility error straight away, in either form, so
// the handler can raise the Task and move on to the next table.
func TestRead_TypeIncompatibleFailsWithoutRetry(t *testing.T) {
	positionalFields := sqltypes.MakeTestFields("@1|@2", "int64|int64")
	positionalRows := sqltypes.ResultToProto3(sqltypes.MakeTestResult(positionalFields, "1|2100")).Rows
	for _, tc := range []struct {
		name   string
		stream func() *vstreamClientMock
	}{
		{
			name: "vttablet cannot use column names",
			stream: func() *vstreamClientMock {
				return &vstreamClientMock{recvFn: func() (*vtgatepb.VStreamResponse, error) {
					return nil, status.Error(codes.Unknown, `persistent error in vstream for connect-test/- on tablet zone1-0000000101; giving up: `+
						`target: connect-test.-.primary: vttablet: rpc error: code = Unknown desc = stream (at source tablet) error @ MySQL56/abc:1-11: `+
						"Code: FAILED_PRECONDITION\ncannot use column names in vstream filter as the current table schema for table customers "+
						"is not compatible with the current event for this table in the stream\n\nfailed to build table replication plan for table customers")
				}}
			},
		},
		{
			name: "positional fields",
			stream: func() *vstreamClientMock {
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{{Events: []*binlogdatapb.VEvent{
					{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "connect-test.customers", Fields: positionalFields}},
					{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "connect-test.customers", RowChanges: []*binlogdatapb.RowChange{{After: positionalRows[0]}}}},
				}}}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbl := &dbLogger{}
			ped := connectClient{}
			startCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "BEFORE_DDL"}
			stopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "STOP"}
			streams := 0
			vc := &vstreamConnectionMock{
				vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
					if in.Vgtid.ShardGtids[0].Gtid == "current" {
						return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{{
							Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("customers", stopCursor)},
						}}}, nil
					}
					streams++
					return tc.stream(), nil
				},
			}
			ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) {
				return vc, nil
			}
			mysqlClient := NewTestMysqlClient(func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
				return []MysqlColumn{{Name: "id", Type: "bigint", IsPrimaryKey: true}, {Name: "a", Type: "varchar(50)"}}, nil
			})
			ped.Mysql = &mysqlClient
			records := 0
			_, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id", "a"}, startCursor,
				func(*sqltypes.Result, Operation) error { records++; return nil }, nil, nil)
			assert.Error(t, err)
			assert.True(t, IsVStreamSchemaIncompatibilityError(err))
			assert.Equal(t, 1, streams)
			assert.Equal(t, 0, records)
		})
	}
}

func TestVStreamErrorReason(t *testing.T) {
	var gtids strings.Builder
	gtids.WriteString("MySQL56/")
	for i := 0; i < 90; i++ {
		if i > 0 {
			gtids.WriteString(",")
		}
		gtids.WriteString(fmt.Sprintf("%08x-1111-2222-3333-444455556666:1-%d", i, 1000000+i))
	}
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "vtgate gave up, vterrors rendering",
			err: status.Error(codes.Unknown, `error starting stream from shard GTID keyspace:"ks" shard:"-" gtid:"`+gtids.String()+`": `+
				`persistent error in vstream for ks/- on tablet zone1-0000000101; giving up: target: ks.-.replica: vttablet: `+
				`rpc error: code = Unknown desc = stream (at source tablet) error @ (including the GTID we failed to process) `+gtids.String()+`: `+
				"Code: FAILED_PRECONDITION\ncolumn newcol not found in table t1\n\nfailed to build table replication plan for table t1\n"+
				"failed to parse transaction payload's internal event"),
			want: "Code: FAILED_PRECONDITION column newcol not found in table t1; failed to build table replication plan for table t1; " +
				"failed to parse transaction payload's internal event",
		},
		{
			name: "connector wrapped the error",
			err: errors.Wrap(status.Error(codes.Unknown, "stream error @ MySQL56/abc:1-11: Code: FAILED_PRECONDITION\ncolumn newcol not found in table t1"),
				"PlanetScale VStream cannot continue incremental replication for table t1 after a schema change"),
			want: "Code: FAILED_PRECONDITION column newcol not found in table t1",
		},
		{
			name: "grpc rendering after vttablet",
			err: status.Error(codes.FailedPrecondition, `persistent error in vstream; giving up: target: ks.-.primary: vttablet: `+
				`rpc error: code = FailedPrecondition desc = stream (at source tablet) error @ MySQL56/abc:1-11,def:1-5: `+
				`failed to build table replication plan for table t1: column c not found in table t1`),
			want: "rpc error: code = FailedPrecondition desc = stream (at source tablet) error @ MySQL56/…: " +
				"failed to build table replication plan for table t1: column c not found in table t1",
		},
		{
			name: "no vttablet part",
			err:  errors.New("cannot determine table columns for t1: event has positional column names only (@1)"),
			want: "",
		},
		{
			name: "nil",
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := VStreamErrorReason(tt.err)
			assert.Equal(t, tt.want, got)
			assert.NotContains(t, got, "1111-2222")
		})
	}

	long := VStreamErrorReason(errors.New("vttablet: " + strings.Repeat("x", 1000)))
	assert.Equal(t, maxVStreamErrorReasonLength+1, len([]rune(long)))
	assert.True(t, strings.HasSuffix(long, "…"))
}

// Review finding 1: with state from an older connector version (no saved
// column order) the handler cannot tell whether the missing column was added
// at the end of the table. vttablet reports the last live column(s) beyond the
// event's column count as "not found" for a column added in the middle too, so
// leaving the reported column out would shift the older values (x gets b's
// value, b gets c's). Without the handler's go-ahead Read keeps the re-sync
// error.
func TestRead_MissingColumnNotRetriedWithoutConfirmedColumnOrder(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	startCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "BEFORE_DDL"}
	stopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "STOP"}
	streams := 0
	vc := &vstreamConnectionMock{
		vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
			if in.Vgtid.ShardGtids[0].Gtid == "current" {
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{{
					Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("customers", stopCursor)},
				}}}, nil
			}
			streams++
			return &vstreamClientMock{recvFn: func() (*vtgatepb.VStreamResponse, error) {
				// ADD COLUMN x AFTER a on (id, a, b, c): the 4-column events resolve
				// to (id, a, x, b), so c is the column "not found".
				return nil, status.Error(codes.FailedPrecondition, "vttablet: rpc error: code = FailedPrecondition desc = "+
					"failed to build table replication plan for table customers: column c not found in table customers")
			}}, nil
		},
	}
	ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) {
		return vc, nil
	}
	mysqlClient := NewTestMysqlClient(func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{
			{Name: "id", Type: "bigint", IsPrimaryKey: true},
			{Name: "a", Type: "varchar(64)"},
			{Name: "x", Type: "varchar(64)"},
			{Name: "b", Type: "varchar(64)"},
			{Name: "c", Type: "varchar(64)"},
		}, nil
	})
	ped.Mysql = &mysqlClient

	_, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id", "a", "x", "b", "c"}, startCursor, nil, nil, nil)
	assert.Error(t, err)
	assert.True(t, IsVStreamSchemaIncompatibilityError(err))
	assert.Equal(t, 1, streams, "the column must not be left out without the handler's go-ahead")
}
