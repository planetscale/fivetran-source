package lib

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

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
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", nil, false, tc, onRow, onCursor, nil)
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
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", nil, false, tc, onRow, onCursor, nil)
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

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{}, "customers", nil, false, tc, nil, nil, nil)
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

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{}, "customers", nil, false, tc, nil, nil, nil)
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
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", nil, false, tc, onRow, onCursor, nil)
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
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", nil, false, tc, onRow, onCursor, nil)
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

	sc, err := ped.Read(context.Background(), dbl, ps, "customers", nil, false, tc, onRow, onCursor, nil)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(newTC)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 2, cc.syncFnInvokedCount)
}

func TestRead_SchemaIncompatibilityResetsCursor(t *testing.T) {
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

	// The second peek fails so the loop terminates once the cursor has been
	// reset, letting the test inspect the reset cursor that gets handed back.
	currentCursorRequests := 0
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

	source := PlanetScaleSource{Database: "connect-test", AutoResyncOnSchemaChange: true}
	sc, err := ped.Read(context.Background(), dbl, source, "customers", []string{"id", "before_col", "after_col"}, false, tc, nil, nil, nil)
	assert.ErrorContains(t, err, "peek failed after reset")
	if assert.NotNil(t, sc) {
		cursor, cErr := sc.SerializedCursorToTableCursor()
		assert.NoError(t, cErr)
		assert.Empty(t, cursor.Position)
		assert.Nil(t, cursor.LastKnownPk)
		if assert.NotNil(t, sc.ErrorCode) {
			assert.Equal(t, "SCHEMA_INCOMPATIBILITY_ERROR", *sc.ErrorCode)
		}
		if assert.NotNil(t, sc.ErrorMessage) {
			assert.Contains(t, *sc.ErrorMessage, "historical sync")
		}
	}
	assert.Equal(t, 3, cc.syncFnInvokedCount)
}

// Without the opt-in the connector keeps the pre-existing contract: surface the
// error with recovery guidance and leave the saved cursor untouched.
func TestRead_SchemaIncompatibilityWithoutOptInReturnsError(t *testing.T) {
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

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id", "before_col", "after_col"}, false, tc, nil, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "historical re-sync")
	assert.Contains(t, err.Error(), "auto_resync_on_schema_change")
	assert.True(t, IsVStreamSchemaIncompatibilityError(err))
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 2, cc.syncFnInvokedCount)
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

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id"}, false, tc, nil, nil, nil)
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

			sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", nil, false, tc, tt.onRow, nil, tt.onUpdate)
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

	sc, err := ped.Read(context.Background(), dbl, ps, "customers", nil, false, responses[0].Cursor, onRow, onCursor, nil)

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
			sc, err := ped.Read(ctx, dbl, ps, "customers", tt.requestedColumns, false, tc, onRow, onCursor, nil)
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

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id"}, false, copyCursor, nil, nil, nil)
	assert.NoError(t, err)
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
	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id"}, false, initialCursor, func(*sqltypes.Result, Operation) error {
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
	_, err := ped.Read(ctx, dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id"}, false, startCursor, nil, nil, nil)
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

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id"}, false, copyCursor, nil, nil, nil)
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

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id"}, false, initialCursor, nil, nil, nil)
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

func TestHandleVStreamEvent_ReportsDDLAsSchemaChange(t *testing.T) {
	cursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "DDL_GTID"}

	returned, records, copyCompleted, schemaChanged, err := handleVStreamEvent(
		"customers",
		cursor,
		&binlogdatapb.VEvent{Type: binlogdatapb.VEventType_DDL, Statement: "alter table customers add column new_col int"},
		map[string][]*query.Field{},
		nil,
		nil,
	)

	assert.NoError(t, err)
	assert.True(t, schemaChanged)
	assert.False(t, copyCompleted)
	assert.Equal(t, 0, records)
	assert.True(t, proto.Equal(cursor, returned), "a DDL event must not move the cursor")
}

func TestHandleVStreamEvent_OtherEventsAreNotSchemaChanges(t *testing.T) {
	cursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test"}

	for _, eventType := range []binlogdatapb.VEventType{
		binlogdatapb.VEventType_BEGIN,
		binlogdatapb.VEventType_COMMIT,
		binlogdatapb.VEventType_OTHER,
		binlogdatapb.VEventType_HEARTBEAT,
	} {
		_, _, _, schemaChanged, err := handleVStreamEvent("customers", cursor, &binlogdatapb.VEvent{Type: eventType}, map[string][]*query.Field{}, nil, nil)
		assert.NoError(t, err)
		assert.False(t, schemaChanged, "event %v must not be reported as a schema change", eventType)
	}
}

// A DDL is only a stopping point when Read asked for one. Without stopOnDDL the
// stream must behave exactly as it did before this feature existed.
func TestSync_IgnoresDDLWhenNotAdoptingNewColumns(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}

	startCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test"}
	ddlCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "DDL_GTID"}
	stopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "STOP_GTID"}
	afterStopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "AFTER_STOP_GTID"}

	rawStream := &vstreamClientMock{
		responses: []*vtgatepb.VStreamResponse{
			{Events: []*binlogdatapb.VEvent{
				vstreamVGtidEventFromCursor("connect-test.customers", ddlCursor),
				{Type: binlogdatapb.VEventType_DDL, Statement: "alter table customers add column new_col int"},
			}},
			{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", stopCursor)}},
			{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", afterStopCursor)}},
		},
	}
	vc := &vstreamConnectionMock{
		vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
			return rawStream, nil
		},
	}
	ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) { return vc, nil }

	returnedCursor, err := ped.sync(context.Background(), dbl, "customers", []string{"id", "name"}, false, startCursor, "STOP_GTID", PlanetScaleSource{Database: "connect-test"}, psdbconnect.TabletType_primary, time.Second, nil, nil, nil)

	assert.True(t, errors.Is(err, io.EOF), "expected the stream to run to its stop position, got %v", err)
	assert.False(t, errors.Is(err, errAdoptNewColumns))
	assert.True(t, proto.Equal(afterStopCursor, returnedCursor))
}

func TestSync_StopsAtDDLWhenAdoptingNewColumns(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}

	startCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test"}
	ddlCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "DDL_GTID"}
	testFields := sqltypes.MakeTestFields("id|name", "int64|varbinary")
	rows := sqltypes.ResultToProto3(sqltypes.MakeTestResult(testFields, "1|keep-me")).Rows

	rawStream := &vstreamClientMock{
		responses: []*vtgatepb.VStreamResponse{
			{Events: []*binlogdatapb.VEvent{
				{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{
					TableName: "connect-test.customers",
					Fields:    testFields,
				}},
				// A row written before the DDL: it must still be delivered on the
				// narrow projection rather than being dropped by the early stop.
				{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
					TableName:  "connect-test.customers",
					RowChanges: []*binlogdatapb.RowChange{{After: rows[0]}},
				}},
				vstreamVGtidEventFromCursor("connect-test.customers", ddlCursor),
				{Type: binlogdatapb.VEventType_DDL, Statement: "alter table customers add column new_col int"},
				// Anything after the DDL belongs to the widened projection.
				{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
					TableName:  "connect-test.customers",
					RowChanges: []*binlogdatapb.RowChange{{After: rows[0]}},
				}},
			}},
		},
	}
	vc := &vstreamConnectionMock{
		vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
			return rawStream, nil
		},
	}
	ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) { return vc, nil }

	records := 0
	checkpoints := []*psdbconnect.TableCursor{}
	returnedCursor, err := ped.sync(context.Background(), dbl, "customers", []string{"id", "name"}, true, startCursor, "STOP_GTID", PlanetScaleSource{Database: "connect-test"}, psdbconnect.TabletType_primary, time.Second, func(*sqltypes.Result, Operation) error {
		records++
		return nil
	}, func(cursor *psdbconnect.TableCursor) error {
		checkpoints = append(checkpoints, cloneTableCursor(cursor))
		return nil
	}, nil)

	assert.True(t, errors.Is(err, errAdoptNewColumns), "expected the adopt signal, got %v", err)
	// The cursor must sit at the DDL so the widened projection resolves when the
	// stream resumes -- this is the whole safety property of the DDL gate.
	assert.True(t, proto.Equal(ddlCursor, returnedCursor))
	assert.Equal(t, 1, records, "the pre-DDL row is delivered, the post-DDL row is not")
	if assert.Len(t, checkpoints, 1, "progress must be checkpointed before handing back") {
		assert.True(t, proto.Equal(ddlCursor, checkpoints[0]))
	}
}

func TestRead_WidensProjectionAfterDDL(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}

	startCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test"}
	ddlCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "DDL_GTID"}
	stopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "STOP_GTID"}
	afterStopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "AFTER_STOP_GTID"}

	// The database has a column Fivetran never selected.
	mysqlClient := NewTestMysqlClient(func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id"}, {Name: "name"}, {Name: "new_col"}}, nil
	})
	ped.Mysql = &mysqlClient

	filters := []string{}
	vc := &vstreamConnectionMock{
		vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
			// The peek that Read does before each sync window.
			if in.Vgtid.ShardGtids[0].Gtid == "current" {
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
					{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", stopCursor)}},
				}}, nil
			}

			filters = append(filters, in.Filter.Rules[0].Filter)
			if len(filters) == 1 {
				// First window: stop at the DDL.
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
					{Events: []*binlogdatapb.VEvent{
						vstreamVGtidEventFromCursor("connect-test.customers", ddlCursor),
						{Type: binlogdatapb.VEventType_DDL, Statement: "alter table customers add column new_col int"},
					}},
				}}, nil
			}
			// Second window: run to the stop position.
			return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
				{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", stopCursor)}},
				{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", afterStopCursor)}},
			}}, nil
		},
	}
	ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) { return vc, nil }

	ps := PlanetScaleSource{Database: "connect-test", PropagateNewColumns: true}
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", []string{"id", "name"}, true, startCursor, nil, nil, nil)

	assert.NoError(t, err)
	if assert.Len(t, filters, 2, "expected the stream to restart once after the DDL") {
		assert.Equal(t, "SELECT `id`,`name` FROM `customers`", filters[0], "the first window must not name the unselected column")
		assert.Equal(t, "SELECT `id`,`name`,`new_col` FROM `customers`", filters[1], "the second window must adopt it")
	}
	expected, err := TableCursorToSerializedCursor(afterStopCursor)
	assert.NoError(t, err)
	assert.Equal(t, expected, sc)
}

// The opt-in flag and Fivetran's per-table choice must both be set; either one
// alone leaves the projection alone.
func TestRead_DoesNotWidenProjectionUnlessBothOptInsAreSet(t *testing.T) {
	cases := []struct {
		name                string
		propagateNewColumns bool
		includeNewColumns   bool
	}{
		{name: "both off", propagateNewColumns: false, includeNewColumns: false},
		{name: "connector opt-in only", propagateNewColumns: true, includeNewColumns: false},
		{name: "fivetran selection only", propagateNewColumns: false, includeNewColumns: true},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			dbl := &dbLogger{}
			ped := connectClient{}

			startCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test"}
			ddlCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "DDL_GTID"}
			stopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "STOP_GTID"}
			afterStopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "AFTER_STOP_GTID"}

			mysqlClient := NewTestMysqlClient(func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
				return []MysqlColumn{{Name: "id"}, {Name: "name"}, {Name: "new_col"}}, nil
			})
			ped.Mysql = &mysqlClient

			filters := []string{}
			vc := &vstreamConnectionMock{
				vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
					if in.Vgtid.ShardGtids[0].Gtid == "current" {
						return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
							{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", stopCursor)}},
						}}, nil
					}
					filters = append(filters, in.Filter.Rules[0].Filter)
					// The DDL is present either way; it just must not be acted on.
					return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
						{Events: []*binlogdatapb.VEvent{
							vstreamVGtidEventFromCursor("connect-test.customers", ddlCursor),
							{Type: binlogdatapb.VEventType_DDL, Statement: "alter table customers add column new_col int"},
							vstreamVGtidEventFromCursor("connect-test.customers", stopCursor),
						}},
						{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", afterStopCursor)}},
					}}, nil
				},
			}
			ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) { return vc, nil }

			ps := PlanetScaleSource{Database: "connect-test", PropagateNewColumns: tt.propagateNewColumns}
			_, err := ped.Read(context.Background(), dbl, ps, "customers", []string{"id", "name"}, tt.includeNewColumns, startCursor, nil, nil, nil)

			assert.NoError(t, err)
			if assert.Len(t, filters, 1, "the stream must not restart") {
				assert.Equal(t, "SELECT `id`,`name` FROM `customers`", filters[0])
			}
		})
	}
}

func TestRebuildProjection(t *testing.T) {
	tests := []struct {
		name            string
		liveColumns     []string
		projected       []string
		expectedColumns []string
		expectedAdded   []string
		expectedRemoved []string
		expectedChanged bool
	}{
		{
			name:            "adds columns the database has and Fivetran never named",
			liveColumns:     []string{"id", "name", "added_a", "added_b"},
			projected:       []string{"id", "name"},
			expectedColumns: []string{"id", "name", "added_a", "added_b"},
			expectedAdded:   []string{"added_a", "added_b"},
			expectedRemoved: []string{},
			expectedChanged: true,
		},
		{
			// Naming a dropped column fails the post-DDL replay exactly the way
			// naming a not-yet-existing one fails a pre-DDL replay.
			name:            "removes columns that no longer exist",
			liveColumns:     []string{"id", "name"},
			projected:       []string{"id", "name", "gone"},
			expectedColumns: []string{"id", "name"},
			expectedAdded:   []string{},
			expectedRemoved: []string{"gone"},
			expectedChanged: true,
		},
		{
			name:            "handles an add and a drop in one schema change",
			liveColumns:     []string{"id", "name", "added"},
			projected:       []string{"id", "name", "gone"},
			expectedColumns: []string{"id", "name", "added"},
			expectedAdded:   []string{"added"},
			expectedRemoved: []string{"gone"},
			expectedChanged: true,
		},
		{
			// An index-only DDL still emits a DDL event; it must be a no-op.
			name:            "reports no change when the layout is unchanged",
			liveColumns:     []string{"id", "name"},
			projected:       []string{"id", "name"},
			expectedColumns: []string{"id", "name"},
			expectedAdded:   []string{},
			expectedRemoved: []string{},
			expectedChanged: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ped := connectClient{}
			mysqlClient := NewTestMysqlClient(func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
				columns := make([]MysqlColumn, 0, len(tt.liveColumns))
				for _, c := range tt.liveColumns {
					columns = append(columns, MysqlColumn{Name: c})
				}
				return columns, nil
			})
			ped.Mysql = &mysqlClient

			rebuild, err := ped.rebuildProjection(context.Background(), PlanetScaleSource{Database: "connect-test"}, "customers", tt.projected)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedColumns, rebuild.Columns)
			assert.Equal(t, tt.expectedAdded, rebuild.Added)
			assert.Equal(t, tt.expectedRemoved, rebuild.Removed)
			assert.Equal(t, tt.expectedChanged, rebuild.changed())
		})
	}
}

func TestRebuildProjection_KeepsProjectionOnLookupFailure(t *testing.T) {
	ped := connectClient{}
	mysqlClient := NewTestMysqlClient(func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return nil, errors.New("information_schema unavailable")
	})
	ped.Mysql = &mysqlClient

	rebuild, err := ped.rebuildProjection(context.Background(), PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id", "name"})
	assert.Error(t, err)
	assert.Equal(t, []string{"id", "name"}, rebuild.Columns, "a failed lookup must not narrow or widen the projection")
	assert.False(t, rebuild.changed())
}

// The same DDL gate that makes an added column safe also makes a dropped one
// safe: resuming at the DDL means no pre-DDL row events are replayed, so the
// rebuilt projection just stops naming the column that went away.
func TestRead_NarrowsProjectionAfterDroppedColumn(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}

	startCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test"}
	ddlCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "DDL_GTID"}
	stopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "STOP_GTID"}
	afterStopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "AFTER_STOP_GTID"}

	// "doomed" is selected and exists at the start; the DDL drops it.
	liveColumns := []MysqlColumn{{Name: "id"}, {Name: "name"}, {Name: "doomed"}}
	mysqlClient := NewTestMysqlClient(func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return liveColumns, nil
	})
	ped.Mysql = &mysqlClient

	filters := []string{}
	vc := &vstreamConnectionMock{
		vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
			if in.Vgtid.ShardGtids[0].Gtid == "current" {
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
					{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", stopCursor)}},
				}}, nil
			}

			filters = append(filters, in.Filter.Rules[0].Filter)
			if len(filters) == 1 {
				// The DDL drops the column, so the rebuild sees it gone.
				liveColumns = []MysqlColumn{{Name: "id"}, {Name: "name"}}
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
					{Events: []*binlogdatapb.VEvent{
						vstreamVGtidEventFromCursor("connect-test.customers", ddlCursor),
						{Type: binlogdatapb.VEventType_DDL, Statement: "alter table customers drop column doomed"},
					}},
				}}, nil
			}
			return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
				{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", stopCursor)}},
				{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", afterStopCursor)}},
			}}, nil
		},
	}
	ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) { return vc, nil }

	ps := PlanetScaleSource{Database: "connect-test", PropagateNewColumns: true}
	_, err := ped.Read(context.Background(), dbl, ps, "customers", []string{"id", "name", "doomed"}, true, startCursor, nil, nil, nil)

	assert.NoError(t, err)
	if assert.Len(t, filters, 2, "expected the stream to restart once after the DDL") {
		assert.Equal(t, "SELECT `id`,`name`,`doomed` FROM `customers`", filters[0])
		assert.Equal(t, "SELECT `id`,`name` FROM `customers`", filters[1], "the dropped column must leave the projection")
	}
}

// If a resume ever re-delivered the DDL it just stopped at, the naive loop
// would rebuild forever. The second stop at the same position must disarm
// adoption instead of spinning.
func TestRead_DoesNotSpinWhenDDLRepeatsAtTheSamePosition(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}

	startCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test"}
	ddlCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "DDL_GTID"}
	stopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "STOP_GTID"}
	afterStopCursor := &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: "AFTER_STOP_GTID"}

	mysqlClient := NewTestMysqlClient(func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		return []MysqlColumn{{Name: "id"}, {Name: "name"}, {Name: "new_col"}}, nil
	})
	ped.Mysql = &mysqlClient

	syncWindows := 0
	vc := &vstreamConnectionMock{
		vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
			if in.Vgtid.ShardGtids[0].Gtid == "current" {
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
					{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", stopCursor)}},
				}}, nil
			}

			syncWindows++
			if syncWindows > 10 {
				t.Fatal("Read is spinning on the same DDL")
			}

			// Always re-deliver the same DDL at the same position: the
			// pathological case the guard exists for.
			if syncWindows <= 2 {
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
					{Events: []*binlogdatapb.VEvent{
						vstreamVGtidEventFromCursor("connect-test.customers", ddlCursor),
						{Type: binlogdatapb.VEventType_DDL, Statement: "alter table customers add column new_col int"},
					}},
				}}, nil
			}
			return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
				{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", stopCursor)}},
				{Events: []*binlogdatapb.VEvent{vstreamVGtidEventFromCursor("connect-test.customers", afterStopCursor)}},
			}}, nil
		},
	}
	ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) { return vc, nil }

	ps := PlanetScaleSource{Database: "connect-test", PropagateNewColumns: true}
	_, err := ped.Read(context.Background(), dbl, ps, "customers", []string{"id", "name"}, true, startCursor, nil, nil, nil)

	assert.NoError(t, err)
	// One rebuild, one disarm, then the stream runs to its stop position.
	assert.Equal(t, 3, syncWindows)
}
