package lib

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/proto/query"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

	sc, err := ped.Read(context.Background(), dbl, PlanetScaleSource{Database: "connect-test"}, "customers", []string{"id", "before_col", "after_col"}, tc, nil, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "historical re-sync")
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

const vstreamColumnNotFoundErrorMessage = "error starting stream from shard GTID keyspace:\"fivetran\" shard:\"-\": persistent error in vstream: " +
	"stream (at source tablet) error @ (including the GTID we failed to process): Code: FAILED_PRECONDITION\n" +
	"column after_col not found in table customers\n\n" +
	"failed to build table replication plan for table customers\n" +
	"failed to parse transaction payload's internal event"
