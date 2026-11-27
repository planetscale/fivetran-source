package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/pkg/errors"

	"github.com/planetscale/fivetran-source/lib"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

func TestCallsReadWithSelectedSchema(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "sync-request")
	psc := &lib.PlanetScaleSource{}
	tl := &testLogger{}
	schema := fivetransdk.SchemaSelection{
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
	}
	sync := Sync{}
	schemaSelection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{
			Schemas: []*fivetransdk.SchemaSelection{
				&schema,
			},
		},
	}

	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		assert.Equal(t, "sync-request", ctx.Value(contextKey{}))
		assert.Equal(t, "customers", tableName)
		return nil, nil
	}

	state, err := psc.GetInitialState("SalesDB", []string{"-"})
	assert.NoError(t, err)
	db := lib.NewTestConnectClient(readFn)
	err = sync.Handle(ctx, psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			"SalesDB": {
				Streams: map[string]lib.ShardStates{
					"SalesDB:customers": state,
				},
			},
		},
	}, schemaSelection)
	assert.NoError(t, err)
}

func TestCallsTruncateOnInitialSync(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	tl := &testLogger{}
	schema := fivetransdk.SchemaSelection{
		SchemaName: "SalesDB",
		Included:   true,
		Tables: []*fivetransdk.TableSelection{
			{
				Included:  true,
				TableName: "customers",
			},
		},
	}
	sync := Sync{}
	schemaSelection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{
			Schemas: []*fivetransdk.SchemaSelection{
				&schema,
			},
		},
	}

	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		assert.Equal(t, "customers", tableName)
		return nil, nil
	}

	state, err := psc.GetInitialState("SalesDB", []string{"-"})
	assert.NoError(t, err)

	db := lib.NewTestConnectClient(readFn)
	err = sync.Handle(context.Background(), psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			"SalesDB": {
				Streams: map[string]lib.ShardStates{
					"SalesDB:customers": state,
				},
			},
		},
	}, schemaSelection)
	assert.NoError(t, err)
	assert.True(t, tl.truncateCalled)
}

func TestInitialSyncReturnsErrorWhenTruncateFails(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	tl := &testLogger{truncateErr: assert.AnError}
	schema := fivetransdk.SchemaSelection{
		SchemaName: "SalesDB",
		Included:   true,
		Tables: []*fivetransdk.TableSelection{
			{
				Included:  true,
				TableName: "customers",
			},
		},
	}
	schemaSelection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{
			Schemas: []*fivetransdk.SchemaSelection{
				&schema,
			},
		},
	}

	readCalled := false
	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		readCalled = true
		return nil, nil
	}

	state, err := psc.GetInitialState("SalesDB", []string{"-"})
	assert.NoError(t, err)

	db := lib.NewTestConnectClient(readFn)
	err = (&Sync{}).Handle(context.Background(), psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			"SalesDB": {
				Streams: map[string]lib.ShardStates{
					"SalesDB:customers": state,
				},
			},
		},
	}, schemaSelection)
	assert.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "failed to truncate table customers")
	assert.True(t, tl.truncateCalled)
	assert.False(t, readCalled)
}

func TestCallsReadWithStartingGtids(t *testing.T) {
	psc := &lib.PlanetScaleSource{
		StartingGtids: "{\"SalesDB\":{\"-80\":\"MySQL56/MYGTID:1-3\",\"80-\":\"MySQL56/MYOTHERGTID:1-3\"}}",
	}

	tl := &testLogger{}
	schema := fivetransdk.SchemaSelection{
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
	}
	sync := Sync{}
	schemaSelection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{
			Schemas: []*fivetransdk.SchemaSelection{
				&schema,
			},
		},
	}

	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		assert.Equal(t, "customers", tableName)
		return nil, nil
	}

	state, err := psc.GetInitialState("SalesDB", []string{"-80", "80-"})
	assert.NoError(t, err)

	cursor, err := lib.TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "-80",
		Keyspace: "SalesDB",
		Position: "MySQL56/MYGTID:1-3",
	})
	assert.NoError(t, err)
	assert.Equal(t, state.Shards["-80"], cursor)

	cursor, err = lib.TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "80-",
		Keyspace: "SalesDB",
		Position: "MySQL56/MYOTHERGTID:1-3",
	})
	assert.NoError(t, err)
	assert.Equal(t, state.Shards["80-"], cursor)

	db := lib.NewTestConnectClient(readFn)
	err = sync.Handle(context.Background(), psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			"SalesDB": {
				Streams: map[string]lib.ShardStates{
					"SalesDB:customers": state,
				},
			},
		},
	}, schemaSelection)
	assert.NoError(t, err)
}

func TestVStreamSchemaIncompatibilityRaisesResyncTask(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	tl := &testLogger{}
	schema := fivetransdk.SchemaSelection{
		SchemaName: "SalesDB",
		Included:   true,
		Tables: []*fivetransdk.TableSelection{
			{
				Included:  true,
				TableName: "customers",
			},
		},
	}
	schemaSelection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{
			Schemas: []*fivetransdk.SchemaSelection{
				&schema,
			},
		},
	}

	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		return nil, status.Error(codes.Unknown, "Code: FAILED_PRECONDITION\n"+
			"column after_col not found in table customers\n\n"+
			"failed to build table replication plan for table customers")
	}

	state, err := psc.GetInitialState("SalesDB", []string{"-"})
	assert.NoError(t, err)
	db := lib.NewTestConnectClient(readFn)
	err = (&Sync{}).Handle(context.Background(), psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			"SalesDB": {
				Streams: map[string]lib.ShardStates{
					"SalesDB:customers": state,
				},
			},
		},
	}, schemaSelection)
	assert.NoError(t, err)
	assert.Len(t, tl.tasks, 1)
	assert.Contains(t, tl.tasks[0], "Re-sync required")
	assert.Contains(t, tl.tasks[0], "customers")
}

func TestShardNotFoundInVGTIDRaisesResyncTask(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	tl := &testLogger{}
	schema := fivetransdk.SchemaSelection{
		SchemaName: "prod_main",
		Included:   true,
		Tables:     []*fivetransdk.TableSelection{{Included: true, TableName: "states"}},
	}
	schemaSelection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{Schemas: []*fivetransdk.SchemaSelection{&schema}},
	}
	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		return nil, errors.Wrap(status.Error(codes.FailedPrecondition, `error starting stream from shard GTID keyspace:"prod_main" shard:"-40" gtid:"current": `+
			`failed to determine if keyspace prod_main has been resharded: shard provided in VGTID, -40, not found in the prod_main keyspace`),
			"Unable to get latest cursor position")
	}
	state, err := psc.GetInitialState("prod_main", []string{"-40"})
	assert.NoError(t, err)
	db := lib.NewTestConnectClient(readFn)
	err = (&Sync{}).Handle(context.Background(), psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			"prod_main": {Streams: map[string]lib.ShardStates{"prod_main:states": state}},
		},
	}, schemaSelection)
	assert.NoError(t, err)
	assert.Len(t, tl.tasks, 1)
	assert.Contains(t, tl.tasks[0], `shard "-40"`)
}

func TestBinlogExpirationsSendOneCombinedWarning(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	tl := &testLogger{}
	schema := fivetransdk.SchemaSelection{
		SchemaName: "prod_main",
		Included:   true,
		Tables: []*fivetransdk.TableSelection{
			{Included: true, TableName: "states"},
			{Included: true, TableName: "users"},
		},
	}
	schemaSelection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{Schemas: []*fivetransdk.SchemaSelection{&schema}},
	}
	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		recorder := logger.(lib.BinlogExpirationRecorder)
		// Simulate connectClient.Read resetting the cursor (twice for the same shard).
		recorder.RecordBinlogExpiration(tc.Keyspace, tableName, tc.Shard)
		recorder.RecordBinlogExpiration(tc.Keyspace, tableName, tc.Shard)
		return lib.TableCursorToSerializedCursor(&psdbconnect.TableCursor{Keyspace: tc.Keyspace, Shard: tc.Shard})
	}
	states, err := psc.GetInitialState("prod_main", []string{"-80", "80-"})
	assert.NoError(t, err)
	users, err := psc.GetInitialState("prod_main", []string{"-80"})
	assert.NoError(t, err)
	db := lib.NewTestConnectClient(readFn)
	err = (&Sync{}).Handle(context.Background(), psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			"prod_main": {Streams: map[string]lib.ShardStates{"prod_main:states": states, "prod_main:users": users}},
		},
	}, schemaSelection)
	assert.NoError(t, err)
	if assert.Len(t, tl.warnings, 1) {
		w := tl.warnings[0]
		assert.Contains(t, w, "PlanetScale binary logs expired before Fivetran could read the latest changes for the following table(s): ")
		assert.Contains(t, w, "prod_main.states (shard -80)")
		assert.Contains(t, w, "prod_main.states (shard 80-)")
		assert.Contains(t, w, "prod_main.users (shard -80)")
		assert.Equal(t, 3, strings.Count(w, "(shard "))
		assert.True(t, strings.HasSuffix(w, ". An automatic historical re-sync has been initiated. No action is needed."))
	}
}

func TestNoBinlogExpirationWarningWhenNothingExpired(t *testing.T) {
	tl := &testLogger{}
	assert.NoError(t, tl.FlushBinlogExpirationWarning())
	assert.Empty(t, tl.warnings)
}

func TestBinlogExpirationWarningSentBeforeResyncTask(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	tl := &testLogger{}
	schema := fivetransdk.SchemaSelection{
		SchemaName: "prod_main",
		Included:   true,
		Tables: []*fivetransdk.TableSelection{
			{Included: true, TableName: "states"},
		},
	}
	schemaSelection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{Schemas: []*fivetransdk.SchemaSelection{&schema}},
	}
	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		logger.(lib.BinlogExpirationRecorder).RecordBinlogExpiration(tc.Keyspace, tableName, tc.Shard)
		return nil, status.Error(codes.Unknown, "Code: FAILED_PRECONDITION\n"+
			"column after_col not found in table states\n\n"+
			"failed to build table replication plan for table states")
	}
	state, err := psc.GetInitialState("prod_main", []string{"-"})
	assert.NoError(t, err)
	db := lib.NewTestConnectClient(readFn)
	err = (&Sync{}).Handle(context.Background(), psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			"prod_main": {Streams: map[string]lib.ShardStates{"prod_main:states": state}},
		},
	}, schemaSelection)
	assert.NoError(t, err)
	assert.Len(t, tl.tasks, 1)
	assert.Len(t, tl.warnings, 1, "warning must be flushed exactly once, before the task")
	assert.Equal(t, []string{"warning", "task"}, tl.events)
}

func TestNoBinlogExpirationWarningWhenSyncFails(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	tl := &testLogger{}
	schema := fivetransdk.SchemaSelection{
		SchemaName: "prod_main",
		Included:   true,
		Tables: []*fivetransdk.TableSelection{
			{Included: true, TableName: "states"},
		},
	}
	schemaSelection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{Schemas: []*fivetransdk.SchemaSelection{&schema}},
	}
	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		logger.(lib.BinlogExpirationRecorder).RecordBinlogExpiration(tc.Keyspace, tableName, tc.Shard)
		return nil, status.Error(codes.Unavailable, "connection reset by peer")
	}
	state, err := psc.GetInitialState("prod_main", []string{"-"})
	assert.NoError(t, err)
	db := lib.NewTestConnectClient(readFn)
	err = (&Sync{}).Handle(context.Background(), psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			"prod_main": {Streams: map[string]lib.ShardStates{"prod_main:states": state}},
		},
	}, schemaSelection)
	assert.Error(t, err)
	assert.Empty(t, tl.tasks)
	assert.Empty(t, tl.warnings, "no \"No action is needed\" warning on a failing sync")
}

func TestCheckpointsHistoricalCopyCursorFromRead(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	tl := &testLogger{}
	schemaSelection := &fivetransdk.Selection_WithSchema{
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
					},
				},
			},
		},
	}

	copyCursor := &psdbconnect.TableCursor{
		Shard:       "-",
		Keyspace:    "SalesDB",
		LastKnownPk: testLastKnownPK("42"),
	}
	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		return nil, onCursor(copyCursor)
	}

	state, err := psc.GetInitialState("SalesDB", []string{"-"})
	assert.NoError(t, err)
	db := lib.NewTestConnectClient(readFn)
	err = (&Sync{}).Handle(context.Background(), psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			"SalesDB": {
				Streams: map[string]lib.ShardStates{
					"SalesDB:customers": state,
				},
			},
		},
	}, schemaSelection)
	assert.NoError(t, err)

	assert.True(t, stateLogContainsLastKnownPK(t, tl.states, "SalesDB", "SalesDB:customers", "-"))
}

func TestDoesNotTruncateWhenStateHasHistoricalCopyProgress(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	tl := &testLogger{}
	schemaSelection := &fivetransdk.Selection_WithSchema{
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
					},
				},
			},
		},
	}

	cursor, err := lib.TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:       "-",
		Keyspace:    "SalesDB",
		LastKnownPk: testLastKnownPK("42"),
	})
	assert.NoError(t, err)

	readCalled := false
	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		readCalled = true
		assert.Empty(t, tc.Position)
		assert.NotNil(t, tc.LastKnownPk)
		return nil, nil
	}

	db := lib.NewTestConnectClient(readFn)
	err = (&Sync{}).Handle(context.Background(), psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			"SalesDB": {
				Streams: map[string]lib.ShardStates{
					"SalesDB:customers": {
						Shards: map[string]*lib.SerializedCursor{
							"-": cursor,
						},
					},
				},
			},
		},
	}, schemaSelection)
	assert.NoError(t, err)
	assert.True(t, readCalled)
	assert.False(t, tl.truncateCalled)
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

func stateLogContainsLastKnownPK(t *testing.T, states []lib.SyncState, keyspace string, stream string, shard string) bool {
	t.Helper()

	for _, state := range states {
		keyspaceState, ok := state.Keyspaces[keyspace]
		if !ok {
			continue
		}
		streamState, ok := keyspaceState.Streams[stream]
		if !ok {
			continue
		}
		serializedCursor, ok := streamState.Shards[shard]
		if !ok {
			continue
		}
		cursor, err := serializedCursor.SerializedCursorToTableCursor()
		if err != nil {
			t.Fatalf("deserialize cursor: %v", err)
		}
		if cursor.LastKnownPk != nil {
			return true
		}
	}
	return false
}

// syncOneTable runs Sync.Handle for keyspace "prod_main", table "states", shard "-".
func syncOneTable(t *testing.T, psc *lib.PlanetScaleSource, tl *testLogger, state *lib.SyncState, readFn lib.ReadFunc) error {
	t.Helper()
	schema := fivetransdk.SchemaSelection{
		SchemaName: "prod_main",
		Included:   true,
		Tables:     []*fivetransdk.TableSelection{{Included: true, TableName: "states"}},
	}
	schemaSelection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{Schemas: []*fivetransdk.SchemaSelection{&schema}},
	}
	db := lib.NewTestConnectClient(readFn)
	return (&Sync{}).Handle(context.Background(), psc, &db, tl, state, schemaSelection)
}

func syncStateWithCursor(t *testing.T, position string) *lib.SyncState {
	t.Helper()
	sc, err := lib.TableCursorToSerializedCursor(&psdbconnect.TableCursor{Keyspace: "prod_main", Shard: "-", Position: position})
	assert.NoError(t, err)
	return &lib.SyncState{Keyspaces: map[string]lib.KeyspaceState{
		"prod_main": {Streams: map[string]lib.ShardStates{"prod_main:states": {Shards: map[string]*lib.SerializedCursor{"-": sc}}}},
	}}
}

func suspectedBinlogExpiration(tc *psdbconnect.TableCursor, table string) error {
	return &lib.SuspectedBinlogExpirationError{
		Keyspace: tc.Keyspace, Shard: tc.Shard, Table: table,
		Err: status.Error(codes.Canceled, "failed to find a REPLICA tablet for VStream in prod_main/- within the planetscale_operator_default cell(s): context has expired"),
	}
}

// The exact Task text for prod_main.states shard "-". It must not depend on the
// error text, so Fivetran's content-based de-duplication works across retries.
const binlogExpiredTaskForStates = `Re-sync required: the PlanetScale binary logs needed to resume table "states" in keyspace "prod_main" (shard "-") have expired, ` +
	`so incremental replication cannot continue from the saved position. ` +
	`To fix this, go to the connection's Settings tab in Fivetran and click "Resync all data".`

func TestSuspectedBinlogExpirationFailsOnceThenRaisesTask(t *testing.T) {
	psc := &lib.PlanetScaleSource{UseReplica: true}
	state := syncStateWithCursor(t, "OLD_GTID")
	reads := 0
	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		reads++
		assert.Equal(t, "OLD_GTID", tc.Position, "the cursor must never be reset")
		return nil, suspectedBinlogExpiration(tc, tableName)
	}

	// First sync: a plain error, the cursor is kept and marked.
	tl := &testLogger{}
	err := syncOneTable(t, psc, tl, state, readFn)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "a re-sync Task is raised")
	assert.Empty(t, tl.tasks)
	assert.Empty(t, tl.warnings)
	marked := state.Keyspaces["prod_main"].Streams["prod_main:states"].Shards["-"]
	assert.True(t, marked.HasSuspectedBinlogExpiration())
	assert.Nil(t, marked.ErrorCode, "the #73 error_code field must not be used for the mark")
	assert.Nil(t, marked.ErrorMessage)
	if assert.NotEmpty(t, tl.states) {
		stateJSON, err := json.Marshal(tl.states[len(tl.states)-1])
		assert.NoError(t, err)
		assert.Contains(t, string(stateJSON), `"suspected_binlog_expiration":true`)
		assert.NotContains(t, string(stateJSON), "error_code")
	}

	// Second and third consecutive syncs: the same Task, byte for byte.
	for i := 0; i < 2; i++ {
		tl = &testLogger{}
		assert.NoError(t, syncOneTable(t, psc, tl, state, readFn))
		if assert.Len(t, tl.tasks, 1) {
			assert.Equal(t, binlogExpiredTaskForStates, tl.tasks[0])
		}
		assert.Empty(t, tl.warnings)
	}
	assert.Equal(t, 3, reads)
	tc, err := state.Keyspaces["prod_main"].Streams["prod_main:states"].Shards["-"].SerializedCursorToTableCursor()
	assert.NoError(t, err)
	assert.Equal(t, "OLD_GTID", tc.Position)
}

func TestSuspectedBinlogExpirationMarkClearedBySuccessfulSync(t *testing.T) {
	state := syncStateWithCursor(t, "OLD_GTID")
	state.Keyspaces["prod_main"].Streams["prod_main:states"].Shards["-"].SetSuspectedBinlogExpiration()
	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		return nil, nil
	}
	tl := &testLogger{}
	assert.NoError(t, syncOneTable(t, &lib.PlanetScaleSource{}, tl, state, readFn))
	assert.False(t, state.Keyspaces["prod_main"].Streams["prod_main:states"].Shards["-"].HasSuspectedBinlogExpiration())
	assert.Empty(t, tl.warnings)
}

func TestColumnCountMismatchRaisesResyncTask(t *testing.T) {
	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		return nil, status.Error(codes.Unknown, "stream (at source tablet) error @ abc:1-10: cannot determine table columns for states: event has [3 15 15], schema has [name:\"id\"]")
	}
	tl := &testLogger{}
	assert.NoError(t, syncOneTable(t, &lib.PlanetScaleSource{}, tl, syncStateWithCursor(t, "OLD_GTID"), readFn))
	if assert.Len(t, tl.tasks, 1) {
		assert.Contains(t, tl.tasks[0], "Re-sync required")
		assert.Contains(t, tl.tasks[0], `table "states"`)
	}
}

func TestRowSerializationErrorRaisesTaskWithoutValueOrPrimaryKey(t *testing.T) {
	for _, tinyIntAsBool := range []bool{false, true} {
		readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
			tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
		) (*lib.SerializedCursor, error) {
			// As returned by lib.handleVStreamRows for a failing Record callback.
			return nil, errors.Wrap(&RowSerializationError{
				Column: "is_active", ColumnType: "tinyint(1)",
				PrimaryKey: "org_id=SECRET_ORG, user_id=98765",
				Err:        errors.New(`failed to serialize DataType_BOOLEAN: "SECRET_VALUE"`),
			}, "unable to serialize row")
		}
		tl := &testLogger{}
		err := syncOneTable(t, &lib.PlanetScaleSource{TreatTinyIntAsBoolean: tinyIntAsBool}, tl, syncStateWithCursor(t, "OLD_GTID"), readFn)
		assert.NoError(t, err)
		if assert.Len(t, tl.tasks, 1) {
			task := tl.tasks[0]
			want := `Sync stopped: column "is_active" (MySQL type tinyint(1)) of table "states" in PlanetScale keyspace "prod_main" ` +
				`has a value that can't be converted for Fivetran. Find values that are incompatible with the datatype.`
			if tinyIntAsBool {
				want += ` If this TINYINT(1) column intentionally stores values other than 0 and 1, turn off "Treat tinyint(1) as boolean" ` +
					`in the connection setup instead, then click "Resync all data" on the connection's Settings tab.`
			}
			assert.Equal(t, want, task)
			for _, secret := range []string{"SECRET_VALUE", "SECRET_ORG", "98765"} {
				assert.NotContains(t, task, secret)
			}
			assert.Equal(t, tinyIntAsBool, strings.Contains(task, "Treat tinyint(1) as boolean"))
		}
		// The primary key values go to the connector log, the value itself nowhere.
		var logged []string
		for _, l := range tl.severes {
			if strings.Contains(l, "can't be converted for Fivetran") {
				logged = append(logged, l)
			}
		}
		if assert.Len(t, logged, 1) {
			assert.Equal(t, `value that can't be converted for Fivetran: keyspace "prod_main", table "states", column "is_active" (MySQL type tinyint(1)), `+
				`primary key [org_id=SECRET_ORG, user_id=98765]; the value itself is not logged`, logged[0])
		}
		for _, l := range tl.severes {
			assert.NotContains(t, l, "SECRET_VALUE")
		}
	}
}

func TestSerializationTaskMessageExactText(t *testing.T) {
	rse := &RowSerializationError{Column: "happened_on", ColumnType: "date", PrimaryKey: "id=42"}
	assert.Equal(t,
		`Sync stopped: column "happened_on" (MySQL type date) of table "events" in PlanetScale keyspace "prod_main" has a value that can't be converted for Fivetran. `+
			`Find values that are incompatible with the datatype.`,
		serializationTaskMessage(rse, "prod_main", "events", false))
	// The tinyint(1) hint is only for tinyint(1) columns with the setting on.
	assert.NotContains(t, serializationTaskMessage(rse, "prod_main", "events", true), "TINYINT(1)")
	// Deterministic: the primary key value of the failing row doesn't matter.
	rse2 := *rse
	rse2.PrimaryKey = "id=43"
	assert.Equal(t, serializationTaskMessage(rse, "prod_main", "events", false), serializationTaskMessage(&rse2, "prod_main", "events", false))
}

func TestSendFailureDuringRowIsPlainError(t *testing.T) {
	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		return nil, errors.Wrap(status.Error(codes.Unavailable, "transport is closing"), "unable to serialize row")
	}
	tl := &testLogger{}
	err := syncOneTable(t, &lib.PlanetScaleSource{}, tl, syncStateWithCursor(t, "OLD_GTID"), readFn)
	assert.Error(t, err)
	assert.Empty(t, tl.tasks)
}

func TestFailingTableDoesNotBlockOtherTables(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		wantTask bool
	}{
		{"plain error", status.Error(codes.Unknown, "persistent error in vstream; giving up"), false},
		{"resync task", status.Error(codes.Unknown, "Code: FAILED_PRECONDITION\n"+
			"column after_col not found in table a_broken\n\n"+
			"failed to build table replication plan for table a_broken"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			psc := &lib.PlanetScaleSource{}
			tl := &testLogger{}
			schemaSelection := &fivetransdk.Selection_WithSchema{
				WithSchema: &fivetransdk.TablesWithSchema{
					Schemas: []*fivetransdk.SchemaSelection{{
						SchemaName: "SalesDB",
						Included:   true,
						Tables: []*fivetransdk.TableSelection{
							{Included: true, TableName: "a_broken"},
							{Included: true, TableName: "b_healthy"},
						},
					}},
				},
			}
			var readTables []string
			readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
				cur *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
			) (*lib.SerializedCursor, error) {
				readTables = append(readTables, tableName)
				if tableName == "a_broken" {
					return nil, tc.err
				}
				return nil, nil
			}
			stateA, err := psc.GetInitialState("SalesDB", []string{"-"})
			assert.NoError(t, err)
			stateB, err := psc.GetInitialState("SalesDB", []string{"-"})
			assert.NoError(t, err)
			db := lib.NewTestConnectClient(readFn)
			err = (&Sync{}).Handle(context.Background(), psc, &db, tl, &lib.SyncState{
				Keyspaces: map[string]lib.KeyspaceState{
					"SalesDB": {Streams: map[string]lib.ShardStates{
						"SalesDB:a_broken":  stateA,
						"SalesDB:b_healthy": stateB,
					}},
				},
			}, schemaSelection)
			assert.Equal(t, []string{"a_broken", "b_healthy"}, readTables)
			if tc.wantTask {
				assert.NoError(t, err)
				assert.Len(t, tl.tasks, 1)
				assert.Contains(t, tl.tasks[0], "a_broken")
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "failed to download rows for table : a_broken")
				assert.Empty(t, tl.tasks)
			}
		})
	}
}

// longGTIDSet stands in for the GTID set of a long-lived production database
// (one UUID per primary it ever had), which vtgate puts in front of the reason.
func longGTIDSet() string {
	var b strings.Builder
	b.WriteString("MySQL56/")
	for i := 0; i < 90; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%08x-1111-2222-3333-444455556666:1-%d", i, 1000000+i))
	}
	return b.String()
}

// The e2e case type_incompatible (MODIFY a INT to VARCHAR): vttablet can only
// name the pre-cut-over columns by position, so the table fails and raises the
// re-sync Task. The other table in the sync must still be read and
// checkpointed before the Task goes out, whichever form the failure takes.
func TestTypeIncompatibleTableDoesNotBlockOtherTables(t *testing.T) {
	gtids := longGTIDSet()
	for _, tc := range []struct {
		name       string
		err        error
		wantReason string
	}{
		{
			name: "vttablet cannot use column names, rewrapped by vtgate",
			err: errors.Wrap(status.Error(codes.Unknown, `error starting stream from shard GTID keyspace:"ks" shard:"-" gtid:"`+gtids+`": `+
				`persistent error in vstream for ks/- on tablet zone1-0000000101; giving up: target: ks.-.primary: vttablet: `+
				`rpc error: code = Unknown desc = stream (at source tablet) error @ (including the GTID we failed to process) `+gtids+`: `+
				"Code: FAILED_PRECONDITION\ncannot use column names in vstream filter as the current table schema for table a_type_changed "+
				"is not compatible with the current event for this table in the stream\n\nfailed to build table replication plan for table a_type_changed"),
				"PlanetScale VStream cannot continue incremental replication for table a_type_changed after a schema change; run a Fivetran historical re-sync for this connection to recover"),
			wantReason: "Code: FAILED_PRECONDITION cannot use column names in vstream filter as the current table schema for table a_type_changed",
		},
		{
			name: "positional fields reached the connector",
			err:  errors.New("cannot determine table columns for a_type_changed: event has positional column names only (@1)"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			psc := &lib.PlanetScaleSource{}
			tl := &testLogger{}
			schemaSelection := &fivetransdk.Selection_WithSchema{
				WithSchema: &fivetransdk.TablesWithSchema{
					Schemas: []*fivetransdk.SchemaSelection{{
						SchemaName: "SalesDB",
						Included:   true,
						Tables: []*fivetransdk.TableSelection{
							{Included: true, TableName: "a_type_changed", Columns: map[string]bool{"id": true, "a": true}},
							{Included: true, TableName: "b_bystander", Columns: map[string]bool{"id": true, "label": true}},
						},
					}},
				},
			}
			var readTables []string
			readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
				cur *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
			) (*lib.SerializedCursor, error) {
				readTables = append(readTables, tableName)
				if tableName == "a_type_changed" {
					return nil, tc.err
				}
				return lib.TableCursorToSerializedCursor(&psdbconnect.TableCursor{Keyspace: cur.Keyspace, Shard: cur.Shard, Position: "MySQL56/bystander:1-20"})
			}
			stateA, err := psc.GetInitialState("SalesDB", []string{"-"})
			assert.NoError(t, err)
			stateB, err := psc.GetInitialState("SalesDB", []string{"-"})
			assert.NoError(t, err)
			db := lib.NewTestConnectClient(readFn)
			err = (&Sync{}).Handle(context.Background(), psc, &db, tl, &lib.SyncState{
				Keyspaces: map[string]lib.KeyspaceState{
					"SalesDB": {Streams: map[string]lib.ShardStates{
						"SalesDB:a_type_changed": stateA,
						"SalesDB:b_bystander":    stateB,
					}},
				},
			}, schemaSelection)
			assert.NoError(t, err)
			assert.Equal(t, []string{"a_type_changed", "b_bystander"}, readTables)

			// The bystander's progress is checkpointed before the Task.
			if assert.NotEmpty(t, tl.states) {
				last := tl.states[len(tl.states)-1]
				bc, err := last.Keyspaces["SalesDB"].Streams["SalesDB:b_bystander"].Shards["-"].SerializedCursorToTableCursor()
				assert.NoError(t, err)
				assert.Equal(t, "MySQL56/bystander:1-20", bc.Position)
			}
			if assert.Len(t, tl.tasks, 1) {
				assert.Equal(t, resyncTaskMessage(tc.err, "SalesDB", "a_type_changed", "-"), tl.tasks[0])
				assert.NotContains(t, tl.tasks[0], "MySQL56/")
			}
			assert.Equal(t, []string{"task"}, tl.events)

			// The first log line leads with the table and the reason; the raw
			// error (GTIDs and all) gets a line of its own.
			if assert.NotEmpty(t, tl.severes) {
				assert.True(t, strings.HasPrefix(tl.severes[0], "skipping table SalesDB:a_type_changed for the rest of this sync: "), tl.severes[0])
				assert.NotContains(t, tl.severes[0], "MySQL56/0000")
				if tc.wantReason != "" {
					assert.Contains(t, tl.severes[0], tc.wantReason)
					assert.Len(t, tl.severes, 2)
					assert.Equal(t, "full error for table SalesDB:a_type_changed: "+tc.err.Error(), tl.severes[1])
				} else {
					assert.Equal(t, []string{"skipping table SalesDB:a_type_changed for the rest of this sync: " + tc.err.Error()}, tl.severes)
				}
			}
		})
	}
}

// A sync error must survive Fivetran cutting long messages: it leads with the
// table and vttablet's reason, not the GTID set in front of it.
func TestSyncErrorLeadsWithVttabletReason(t *testing.T) {
	gtids := longGTIDSet()
	raw := status.Error(codes.Unknown, `error starting stream from shard GTID keyspace:"ks" shard:"-" gtid:"`+gtids+`": `+
		`persistent error in vstream for ks/- on tablet zone1-0000000101; giving up: target: ks.-.replica: vttablet: `+
		`rpc error: code = Unknown desc = stream (at source tablet) error @ (including the GTID we failed to process) `+gtids+`: `+
		"Code: RESOURCE_EXHAUSTED\ntoo many concurrent streams")
	psc := &lib.PlanetScaleSource{}
	tl := &testLogger{}
	schemaSelection := &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{
			Schemas: []*fivetransdk.SchemaSelection{{
				SchemaName: "SalesDB",
				Included:   true,
				Tables:     []*fivetransdk.TableSelection{{Included: true, TableName: "customers"}},
			}},
		},
	}
	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		cur *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		return nil, raw
	}
	state, err := psc.GetInitialState("SalesDB", []string{"-"})
	assert.NoError(t, err)
	db := lib.NewTestConnectClient(readFn)
	err = (&Sync{}).Handle(context.Background(), psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{"SalesDB": {Streams: map[string]lib.ShardStates{"SalesDB:customers": state}}},
	}, schemaSelection)
	assert.Error(t, err)
	assert.Empty(t, tl.tasks)
	assert.Equal(t, "rpc error: code = Internal desc = failed to download rows for table : customers , error : Code: RESOURCE_EXHAUSTED too many concurrent streams", err.Error())
	assert.Equal(t, []string{
		"skipping table SalesDB:customers for the rest of this sync: Code: RESOURCE_EXHAUSTED too many concurrent streams",
		"full error for table SalesDB:customers: " + raw.Error(),
	}, tl.severes)
}

// orderedTestLogger is a testLogger that knows the live column order of the
// source tables, like schemaAwareSerializer does.
type orderedTestLogger struct {
	*testLogger
	columns map[string][]string // "keyspace:table" -> live column order
	infos   []string
}

func (l *orderedTestLogger) Info(s string) error {
	l.infos = append(l.infos, s)
	return nil
}

func (l *orderedTestLogger) SourceColumnOrder(keyspace, table string) ([]string, bool) {
	c, ok := l.columns[keyspace+":"+table]
	return c, ok
}

var _ SourceColumnOrderer = (*orderedTestLogger)(nil)

const columnOrderTaskForAddAfter = `Re-sync required: a schema change on table "a_add_after" in PlanetScale keyspace "SalesDB" (for example a column added in the middle ` +
	`of the table, or a dropped or renamed column) prevents incremental replication from continuing. ` +
	`To fix this, go to the connection's Settings tab in Fivetran and click "Resync all data".`

func columnOrderSelection(tables ...string) *fivetransdk.Selection_WithSchema {
	var ts []*fivetransdk.TableSelection
	for _, t := range tables {
		// Only some columns are selected in Fivetran; the check uses the full
		// table order regardless.
		ts = append(ts, &fivetransdk.TableSelection{Included: true, TableName: t, Columns: map[string]bool{"id": true, "b": true}})
	}
	return &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{
			Schemas: []*fivetransdk.SchemaSelection{{SchemaName: "SalesDB", Included: true, Tables: ts}},
		},
	}
}

// streamWithProgress returns the state of one table with a saved cursor on
// every given shard and the given saved column order.
func streamWithProgress(t *testing.T, order []string, positions map[string]string) lib.ShardStates {
	t.Helper()
	ss := lib.ShardStates{Shards: map[string]*lib.SerializedCursor{}, ColumnOrder: order}
	for shard, pos := range positions {
		sc, err := lib.TableCursorToSerializedCursor(&psdbconnect.TableCursor{Keyspace: "SalesDB", Shard: shard, Position: pos})
		assert.NoError(t, err)
		ss.Shards[shard] = sc
	}
	return ss
}

func recordingReadFn(readTables *[]string) lib.ReadFunc {
	return func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		cur *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		*readTables = append(*readTables, tableName+"/"+cur.Shard)
		return lib.TableCursorToSerializedCursor(&psdbconnect.TableCursor{Keyspace: cur.Keyspace, Shard: cur.Shard, Position: "MySQL56/" + tableName + ":1-20"})
	}
}

func lastPosition(t *testing.T, states []lib.SyncState, stream, shard string) string {
	t.Helper()
	if !assert.NotEmpty(t, states) {
		return ""
	}
	tc, err := states[len(states)-1].Keyspaces["SalesDB"].Streams[stream].Shards[shard].SerializedCursorToTableCursor()
	assert.NoError(t, err)
	return tc.Position
}

// The e2e case add_after: ALTER TABLE ... ADD COLUMN newcol AFTER a after the
// saved cursor. The table must not be streamed (its pre-cut-over values would
// land under the wrong columns): exactly one Task, while the other table is
// still read and checkpointed before the Task goes out. The saved order is
// kept, so the next sync raises the same Task until the connection is
// re-synced; after "Resync all data" (empty state) the order is recorded again
// and no Task is raised.
func TestColumnAddedMidTableRaisesOneTaskAndOtherTableSyncs(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	saved := []string{"id", "a", "b", "c"}
	live := []string{"id", "a", "newcol", "b", "c"}
	newLogger := func() *orderedTestLogger {
		return &orderedTestLogger{testLogger: &testLogger{}, columns: map[string][]string{
			"SalesDB:a_add_after": live,
			"SalesDB:b_bystander": {"id", "b"},
		}}
	}
	state := &lib.SyncState{Keyspaces: map[string]lib.KeyspaceState{
		"SalesDB": {Streams: map[string]lib.ShardStates{
			"SalesDB:a_add_after": streamWithProgress(t, saved, map[string]string{"-": "MySQL56/old:1-10"}),
			"SalesDB:b_bystander": streamWithProgress(t, []string{"id", "b"}, map[string]string{"-": "MySQL56/old:1-10"}),
		}},
	}}

	for sync := 1; sync <= 2; sync++ {
		tl := newLogger()
		var readTables []string
		db := lib.NewTestConnectClient(recordingReadFn(&readTables))
		err := (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander"))
		assert.NoError(t, err)
		assert.Equal(t, []string{"b_bystander/-"}, readTables, "sync %d: the changed table must not be streamed", sync)
		if assert.Len(t, tl.tasks, 1, "sync %d", sync) {
			assert.Equal(t, columnOrderTaskForAddAfter, tl.tasks[0])
		}
		assert.Equal(t, []string{"task"}, tl.events)
		assert.False(t, tl.truncateCalled)
		assert.Equal(t, "MySQL56/b_bystander:1-20", lastPosition(t, tl.states, "SalesDB:b_bystander", "-"))
		assert.Equal(t, "MySQL56/old:1-10", lastPosition(t, tl.states, "SalesDB:a_add_after", "-"), "the changed table's cursor is kept")
		last := tl.states[len(tl.states)-1]
		assert.Equal(t, saved, last.Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].ColumnOrder, "the saved order is kept until a re-sync")
		if assert.Len(t, tl.severes, 1) {
			assert.Equal(t, "skipping table SalesDB:a_add_after for the rest of this sync: column order of table a_add_after changed since the last sync "+
				"(new column newcol was added before existing column b), so binlog events from before the change would be decoded into the wrong columns; "+
				"saved order: id,a,b,c; current order: id,a,newcol,b,c", tl.severes[0])
		}
	}

	// "Resync all data": Fivetran sends no state, so StateFromRequest starts
	// every table from GetInitialState (no cursor, no saved order).
	initial, err := psc.GetInitialState("SalesDB", []string{"-"})
	assert.NoError(t, err)
	bystander, err := psc.GetInitialState("SalesDB", []string{"-"})
	assert.NoError(t, err)
	state = &lib.SyncState{Keyspaces: map[string]lib.KeyspaceState{
		"SalesDB": {Streams: map[string]lib.ShardStates{"SalesDB:a_add_after": initial, "SalesDB:b_bystander": bystander}},
	}}
	tl := newLogger()
	var readTables []string
	db := lib.NewTestConnectClient(recordingReadFn(&readTables))
	assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
	assert.Empty(t, tl.tasks)
	assert.Equal(t, []string{"a_add_after/-", "b_bystander/-"}, readTables)
	stateJSON, err := json.Marshal(tl.states[len(tl.states)-1])
	assert.NoError(t, err)
	assert.Contains(t, string(stateJSON), `"column_order":["id","a","newcol","b","c"]`)

	// The next incremental sync compares against the refreshed order.
	tl = newLogger()
	readTables = nil
	assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
	assert.Empty(t, tl.tasks)
	assert.Equal(t, []string{"a_add_after/-", "b_bystander/-"}, readTables)
}

func TestColumnOrderCheck(t *testing.T) {
	saved := []string{"id", "a", "b", "c"}
	for _, tc := range []struct {
		name      string
		saved     []string
		live      []string
		progress  bool
		wantTask  bool
		wantOrder []string // saved order after the sync
	}{
		{name: "identical", saved: saved, live: saved, progress: true, wantOrder: saved},
		{name: "appended at end", saved: saved, live: []string{"id", "a", "b", "c", "d"}, progress: true, wantTask: true, wantOrder: saved},
		{name: "state without saved order", saved: nil, live: []string{"id", "newcol", "a", "b", "c"}, progress: true, wantOrder: []string{"id", "newcol", "a", "b", "c"}},
		{name: "dropped", saved: saved, live: []string{"id", "a", "c"}, progress: true, wantTask: true, wantOrder: saved},
		{name: "renamed in place", saved: saved, live: []string{"id", "a", "b2", "c"}, progress: true, wantTask: true, wantOrder: saved},
		{name: "reordered", saved: saved, live: []string{"id", "a", "c", "b"}, progress: true, wantTask: true, wantOrder: saved},
		{name: "added first", saved: saved, live: []string{"x", "id", "a", "b", "c"}, progress: true, wantTask: true, wantOrder: saved},
		{name: "no progress: copied from scratch, order recorded", saved: saved, live: []string{"id", "a", "c", "b"}, progress: false, wantOrder: []string{"id", "a", "c", "b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			psc := &lib.PlanetScaleSource{}
			pos := ""
			if tc.progress {
				pos = "MySQL56/old:1-10"
			}
			state := &lib.SyncState{Keyspaces: map[string]lib.KeyspaceState{
				"SalesDB": {Streams: map[string]lib.ShardStates{"SalesDB:t": streamWithProgress(t, tc.saved, map[string]string{"-": pos})}},
			}}
			tl := &orderedTestLogger{testLogger: &testLogger{}, columns: map[string][]string{"SalesDB:t": tc.live}}
			var readTables []string
			db := lib.NewTestConnectClient(recordingReadFn(&readTables))
			assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("t")))
			if tc.wantTask {
				assert.Len(t, tl.tasks, 1)
				assert.Empty(t, readTables)
			} else {
				assert.Empty(t, tl.tasks)
				assert.Equal(t, []string{"t/-"}, readTables)
			}
			assert.Equal(t, !tc.progress, tl.truncateCalled)
			if assert.NotEmpty(t, tl.states) {
				assert.Equal(t, tc.wantOrder, tl.states[len(tl.states)-1].Keyspaces["SalesDB"].Streams["SalesDB:t"].ColumnOrder)
			}
		})
	}
}

// The order is saved per table, not per shard: a sharded table raises one Task
// (its text names no shard) and none of its shards is streamed, even if only
// some shards have a cursor.
func TestColumnOrderChangeOnShardedTableRaisesOneTask(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := &lib.SyncState{Keyspaces: map[string]lib.KeyspaceState{
		"SalesDB": {Streams: map[string]lib.ShardStates{
			"SalesDB:a_add_after": streamWithProgress(t, []string{"id", "a", "b"}, map[string]string{"-80": "MySQL56/old:1-10", "80-": ""}),
		}},
	}}
	tl := &orderedTestLogger{testLogger: &testLogger{}, columns: map[string][]string{"SalesDB:a_add_after": {"id", "newcol", "a", "b"}}}
	var readTables []string
	db := lib.NewTestConnectClient(recordingReadFn(&readTables))
	assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after")))
	assert.Empty(t, readTables)
	if assert.Len(t, tl.tasks, 1) {
		assert.Equal(t, columnOrderTaskForAddAfter, tl.tasks[0])
		assert.NotContains(t, tl.tasks[0], "shard")
	}
}

// A Serializer that doesn't know the source schema (or a table missing from
// it) skips the check and leaves the saved order alone.
func TestColumnOrderCheckSkippedWithoutSourceSchema(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	saved := []string{"id", "a", "b"}
	state := &lib.SyncState{Keyspaces: map[string]lib.KeyspaceState{
		"SalesDB": {Streams: map[string]lib.ShardStates{"SalesDB:t": streamWithProgress(t, saved, map[string]string{"-": "MySQL56/old:1-10"})}},
	}}
	for _, logger := range []Serializer{
		&testLogger{},
		&orderedTestLogger{testLogger: &testLogger{}, columns: map[string][]string{}},
	} {
		var readTables []string
		db := lib.NewTestConnectClient(recordingReadFn(&readTables))
		assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, logger, state, columnOrderSelection("t")))
		assert.Equal(t, []string{"t/-"}, readTables)
		assert.Equal(t, saved, state.Keyspaces["SalesDB"].Streams["SalesDB:t"].ColumnOrder)
	}
}

func TestSchemaAwareSerializerSourceColumnOrder(t *testing.T) {
	sb := NewSchemaBuilder(false)
	sb.OnKeyspace("SalesDB")
	sb.OnTable("SalesDB", "customers")
	sb.OnColumns("SalesDB", "customers", []lib.MysqlColumn{
		{Name: "id", Type: "bigint", IsPrimaryKey: true},
		{Name: "newcol", Type: "int"},
		{Name: "email", Type: "varchar(255)"},
	})
	schema, err := sb.(*FiveTranSchemaBuilder).BuildUpdateResponse()
	assert.NoError(t, err)
	s := NewSchemaAwareSerializer(&testLogSender{}, "", false, schema.SchemaList, schema.EnumsAndSets)
	orderer, ok := s.(SourceColumnOrderer)
	if !assert.True(t, ok) {
		return
	}
	columns, ok := orderer.SourceColumnOrder("SalesDB", "customers")
	assert.True(t, ok)
	assert.Equal(t, []string{"id", "newcol", "email"}, columns)
	_, ok = orderer.SourceColumnOrder("SalesDB", "missing")
	assert.False(t, ok)
	_, ok = orderer.SourceColumnOrder("OtherDB", "customers")
	assert.False(t, ok)
}

func TestColumnOrderChangedErrorTaskHasNoColumnNames(t *testing.T) {
	err := &ColumnOrderChangedError{
		Keyspace: "SalesDB", Table: "a_add_after", Reason: "new column newcol was added before existing column b",
		Saved: []string{"id", "a", "b"}, Current: []string{"id", "newcol", "a", "b"},
	}
	task := resyncTaskMessage(err, "SalesDB", "a_add_after", "")
	assert.Equal(t, columnOrderTaskForAddAfter, task)
	assert.False(t, strings.Contains(task, "newcol"))
}

const columnOrderTrackerTaskForAddAfter = `Re-sync required: a schema change on table "a_add_after" in PlanetScale keyspace "SalesDB" changed its columns ` +
	`(a column added, dropped, renamed or moved), and the keyspace does not track ` +
	`schema versions, so changes made before the schema change cannot be read with the right column names. ` +
	`To fix this, enable the VTTablet parameter track_schema_versions for keyspace "SalesDB" in PlanetScale (with schema-version-max-age-seconds ` +
	`set to 0 or at least as long as the binary log retention), then go to the connection's Settings tab in Fivetran and click "Resync all data".`

// scanByShard answers ScanSchemaHistory per shard and records the cursors it
// was asked about.
func scanByShard(verdicts map[string]lib.SchemaHistory, errs map[string]error, scanned *[]string) lib.ScanSchemaFunc {
	return func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, tc *psdbconnect.TableCursor, resume *lib.SchemaScanProgress) (lib.SchemaHistory, error) {
		*scanned = append(*scanned, tableName+"/"+tc.Shard+"@"+tc.Position)
		if err := errs[tc.Shard]; err != nil {
			return lib.SchemaHistory{}, err
		}
		return verdicts[tc.Shard], nil
	}
}

func historianTestState(t *testing.T, saved []string, positions map[string]string) *lib.SyncState {
	return &lib.SyncState{Keyspaces: map[string]lib.KeyspaceState{
		"SalesDB": {Streams: map[string]lib.ShardStates{
			"SalesDB:a_add_after": streamWithProgress(t, saved, positions),
			"SalesDB:b_bystander": streamWithProgress(t, []string{"id", "b"}, map[string]string{"-": "MySQL56/old:1-10"}),
		}},
	}}
}

func historianTestLogger(live []string) *orderedTestLogger {
	return &orderedTestLogger{testLogger: &testLogger{}, columns: map[string][]string{
		"SalesDB:a_add_after": live,
		"SalesDB:b_bystander": {"id", "b"},
	}}
}

var (
	historianSaved = []string{"id", "a", "b", "c"}
	historianLive  = []string{"id", "a", "newcol", "b", "c"}
	trackedDDL     = lib.SchemaHistory{Verdict: lib.SchemaHistoryTracked, DDL: "RENAME TABLE ..."}
	untrackedDDL   = lib.SchemaHistory{Verdict: lib.SchemaHistoryUntracked, DDL: "RENAME TABLE `a_add_after` TO ..."}
	noDDL          = lib.SchemaHistory{Verdict: lib.SchemaHistoryNoDDL}
)

// Historian on: the column order changed, but the binlog has the schema change
// followed by a VERSION event, so the table is streamed as usual (no Task) and
// the new order is recorded, so the next sync does not scan again.
func TestColumnOrderChangeWithSchemaVersionSyncs(t *testing.T) {
	for name, verdict := range map[string]lib.SchemaHistory{"tracked": trackedDDL, "cursor past the change": noDDL} {
		t.Run(name, func(t *testing.T) {
			psc := &lib.PlanetScaleSource{}
			state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
			tl := historianTestLogger(historianLive)
			var readTables, scanned []string
			db := lib.NewTestConnectClient(recordingReadFn(&readTables))
			db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(map[string]lib.SchemaHistory{"-": verdict}, nil, &scanned)
			assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
			assert.Empty(t, tl.tasks)
			assert.Empty(t, tl.severes)
			assert.Equal(t, []string{"a_add_after/-@MySQL56/old:1-10"}, scanned)
			assert.Equal(t, []string{"a_add_after/-", "b_bystander/-"}, readTables)
			assert.Equal(t, historianLive, tl.states[len(tl.states)-1].Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].ColumnOrder)
			assert.Contains(t, tl.infos, "table SalesDB:a_add_after shard -: "+verdict.Verdict.String())

			scanned, readTables = nil, nil
			tl = historianTestLogger(historianLive)
			assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
			assert.Empty(t, scanned, "the order was recorded, so the next sync does not scan")
			assert.Empty(t, tl.tasks)
		})
	}
}

// Historian off: the schema change has no VERSION event after it. Exactly one
// Task, telling the customer to enable track_schema_versions and re-sync; the
// changed table is not streamed and keeps its cursor and saved order, the
// other table syncs and is checkpointed before the Task.
func TestColumnOrderChangeWithoutSchemaVersionRaisesTrackerTask(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	for sync := 1; sync <= 2; sync++ {
		tl := historianTestLogger(historianLive)
		var readTables, scanned []string
		db := lib.NewTestConnectClient(recordingReadFn(&readTables))
		db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(map[string]lib.SchemaHistory{"-": untrackedDDL}, nil, &scanned)
		assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
		assert.Equal(t, []string{"b_bystander/-"}, readTables, "sync %d", sync)
		if assert.Len(t, tl.tasks, 1) {
			assert.Equal(t, columnOrderTrackerTaskForAddAfter, tl.tasks[0])
			assert.NotContains(t, tl.tasks[0], "newcol")
		}
		assert.Equal(t, "MySQL56/b_bystander:1-20", lastPosition(t, tl.states, "SalesDB:b_bystander", "-"))
		assert.Equal(t, "MySQL56/old:1-10", lastPosition(t, tl.states, "SalesDB:a_add_after", "-"))
		assert.Equal(t, historianSaved, tl.states[len(tl.states)-1].Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].ColumnOrder)
		if assert.Len(t, tl.severes, 1) {
			assert.Contains(t, tl.severes[0], "skipping table SalesDB:a_add_after for the rest of this sync: column order of table a_add_after changed")
			assert.Contains(t, tl.severes[0], "shard - has the schema change \"RENAME TABLE `a_add_after` TO ...\" with no schema version after it (track_schema_versions is off)")
		}
	}
}

// Sharded: every shard with a saved position is scanned in shard order (a
// shard without one is skipped). One untracked shard is enough for the Task
// (one Task, no shard in its text) and no shard of the table is streamed;
// when every shard is tracked, every shard is streamed.
func TestColumnOrderChangeSchemaHistoryMultiShard(t *testing.T) {
	positions := map[string]string{"-40": "MySQL56/s1:1-10", "40-80": "", "80-": "MySQL56/s3:1-10"}
	for _, tc := range []struct {
		name        string
		verdicts    map[string]lib.SchemaHistory
		wantTask    bool
		wantScanned []string
		wantRead    []string
	}{
		{
			name:        "every shard tracked",
			verdicts:    map[string]lib.SchemaHistory{"-40": trackedDDL, "80-": trackedDDL},
			wantScanned: []string{"a_add_after/-40@MySQL56/s1:1-10", "a_add_after/80-@MySQL56/s3:1-10"},
			wantRead:    []string{"a_add_after/-40", "a_add_after/40-80", "a_add_after/80-"},
		},
		{
			name:        "one shard past the change, one tracked",
			verdicts:    map[string]lib.SchemaHistory{"-40": noDDL, "80-": trackedDDL},
			wantScanned: []string{"a_add_after/-40@MySQL56/s1:1-10", "a_add_after/80-@MySQL56/s3:1-10"},
			wantRead:    []string{"a_add_after/-40", "a_add_after/40-80", "a_add_after/80-"},
		},
		{
			name:        "last shard untracked",
			verdicts:    map[string]lib.SchemaHistory{"-40": trackedDDL, "80-": untrackedDDL},
			wantTask:    true,
			wantScanned: []string{"a_add_after/-40@MySQL56/s1:1-10", "a_add_after/80-@MySQL56/s3:1-10"},
		},
		{
			name:        "first shard untracked stops the scan",
			verdicts:    map[string]lib.SchemaHistory{"-40": untrackedDDL, "80-": trackedDDL},
			wantTask:    true,
			wantScanned: []string{"a_add_after/-40@MySQL56/s1:1-10"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			psc := &lib.PlanetScaleSource{}
			state := &lib.SyncState{Keyspaces: map[string]lib.KeyspaceState{
				"SalesDB": {Streams: map[string]lib.ShardStates{"SalesDB:a_add_after": streamWithProgress(t, historianSaved, positions)}},
			}}
			tl := historianTestLogger(historianLive)
			var readTables, scanned []string
			db := lib.NewTestConnectClient(recordingReadFn(&readTables))
			db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(tc.verdicts, nil, &scanned)
			assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after")))
			assert.Equal(t, tc.wantScanned, scanned)
			slices.Sort(readTables)
			if tc.wantTask {
				assert.Empty(t, readTables)
				if assert.Len(t, tl.tasks, 1) {
					assert.Equal(t, columnOrderTrackerTaskForAddAfter, tl.tasks[0])
					assert.NotContains(t, tl.tasks[0], "shard")
				}
			} else {
				assert.Empty(t, tl.tasks)
				assert.Equal(t, tc.wantRead, readTables)
			}
		})
	}
}

// The scan could not tell yet (a schema change at the current position the
// tracker has not recorded yet) or failed: a plain sync error, no Task, so
// Fivetran retries and the next sync scans again. The other table still syncs.
func TestColumnOrderChangeSchemaHistoryInconclusiveIsPlainError(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	tl := historianTestLogger(historianLive)
	var readTables, scanned []string
	db := lib.NewTestConnectClient(recordingReadFn(&readTables))
	db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(nil, map[string]error{"-": lib.ErrSchemaHistoryInconclusive}, &scanned)
	err := (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander"))
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "failed to download rows for table : a_add_after")
		assert.Contains(t, err.Error(), lib.ErrSchemaHistoryInconclusive.Error())
	}
	assert.Empty(t, tl.tasks)
	assert.Equal(t, []string{"b_bystander/-"}, readTables)
	assert.Equal(t, historianSaved, tl.states[len(tl.states)-1].Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].ColumnOrder)
}

func TestColumnOrderTrackerTaskText(t *testing.T) {
	err := &ColumnOrderChangedError{
		Keyspace: "SalesDB", Table: "a_add_after", Reason: "new column newcol was added before existing column b",
		Saved: historianSaved, Current: historianLive, Untracked: true, Shard: "-", DDL: "RENAME TABLE x",
	}
	assert.Equal(t, columnOrderTrackerTaskForAddAfter, resyncTaskMessage(err, "SalesDB", "a_add_after", "-"))
}

// Review finding 3: a column order that passes the check (here drop-only) is
// only saved once the table's read completed. If it were saved before a read
// that fails (cursor kept before the DROP), a later ADD COLUMN at the end
// would look like a plain append to the saved order while the cursor is still
// behind the DROP, and the older events would be decoded shifted.
func TestColumnOrderSavedOnlyAfterReadCompletes(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	saved := []string{"id", "a", "b", "c"}
	state := &lib.SyncState{Keyspaces: map[string]lib.KeyspaceState{
		"SalesDB": {Streams: map[string]lib.ShardStates{"SalesDB:t": streamWithProgress(t, saved, map[string]string{"-": "MySQL56/old:1-10"})}},
	}}
	tl := &orderedTestLogger{testLogger: &testLogger{}, columns: map[string][]string{"SalesDB:t": {"id", "a", "c"}}}
	db := lib.NewTestConnectClient(func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		cur *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		return nil, errors.New("vttablet: rpc error: code = Unavailable desc = no healthy tablet")
	})
	var scanned []string
	db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(map[string]lib.SchemaHistory{"-": noDDL}, nil, &scanned)
	assert.Error(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("t")))
	assert.Equal(t, saved, state.Keyspaces["SalesDB"].Streams["SalesDB:t"].ColumnOrder)
	if assert.NotEmpty(t, tl.states) {
		assert.Equal(t, saved, tl.states[len(tl.states)-1].Keyspaces["SalesDB"].Streams["SalesDB:t"].ColumnOrder)
	}
}

// Review finding 4: binlogs purged at the saved position make the schema
// history scan fail the same way Read does. Read resets that shard's cursor
// and copies the table again (nothing is decoded from before the change), so
// the scan error must not skip the table forever.
func TestColumnOrderChangeWithPurgedBinlogsLetsReadResetTheCursor(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	tl := historianTestLogger(historianLive)
	var readTables, readFrom, scanned []string
	record := recordingReadFn(&readTables)
	db := lib.NewTestConnectClient(func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		cur *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		readFrom = append(readFrom, tableName+"@"+cur.Position)
		return record(ctx, logger, ps, tableName, columns, cur, onResult, onCursor, onUpdate)
	})
	purged := errors.New("rpc error: code = Unknown desc = Cannot replicate because the source purged required binary logs.")
	db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(nil, map[string]error{"-": purged}, &scanned)
	assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
	assert.Empty(t, tl.tasks)
	assert.Equal(t, []string{"a_add_after/-", "b_bystander/-"}, readTables)
	assert.Equal(t, []string{"a_add_after@", "b_bystander@MySQL56/old:1-10"}, readFrom, "the expired shard is read from an empty cursor (copied again)")
	// Round 3 item 4: every shard of the table is copied again, so the
	// table is truncated first (rows deleted at the source meanwhile go).
	assert.True(t, tl.truncateCalled)
	assert.Equal(t, historianLive, tl.states[len(tl.states)-1].Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].ColumnOrder)
}

// Review finding 4: a scan error that raises a Task names the shard that
// failed, not "".
func TestColumnOrderChangeScanShardNotFoundTaskNamesShard(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-40": "MySQL56/s1:1-10", "40-": "MySQL56/s2:1-10"})
	tl := historianTestLogger(historianLive)
	var readTables, scanned []string
	db := lib.NewTestConnectClient(recordingReadFn(&readTables))
	gone := errors.New("rpc error: code = InvalidArgument desc = shard provided in VGTID, -40, not found in the SalesDB keyspace")
	db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(nil, map[string]error{"-40": gone}, &scanned)
	assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after")))
	if assert.Len(t, tl.tasks, 1) {
		assert.Contains(t, tl.tasks[0], `shard "-40" no longer exists`)
	}
}

// Review finding 6: an error that affects the whole connection (rejected
// credentials, vtgate unreachable) stops the sync instead of trying every
// remaining table.
func TestConnectionWideErrorStopsTheSync(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	tl := historianTestLogger(historianSaved)
	var readTables []string
	db := lib.NewTestConnectClient(func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		cur *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		readTables = append(readTables, tableName+"/"+cur.Shard)
		return nil, errors.Wrap(status.Error(codes.Unauthenticated, "invalid credentials"), "Unable to get latest cursor position")
	})
	err := (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander"))
	assert.Error(t, err)
	assert.Equal(t, []string{"a_add_after/-"}, readTables)
}

// Review finding 3 (and round 3 item 1): a Read that gives up after repeated
// timeouts, or whose stream vtgate ended early, keeps the cursor it reached,
// but the live order is not saved (the cursor may still be before the schema
// change).
func TestColumnOrderNotSavedWhenReadStopsEarly(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	saved := []string{"id", "a", "b", "c"}
	state := &lib.SyncState{Keyspaces: map[string]lib.KeyspaceState{
		"SalesDB": {Streams: map[string]lib.ShardStates{"SalesDB:t": streamWithProgress(t, saved, map[string]string{"-": "MySQL56/old:1-10"})}},
	}}
	tl := &orderedTestLogger{testLogger: &testLogger{}, columns: map[string][]string{"SalesDB:t": {"id", "a", "b", "c", "d"}}}
	db := lib.NewTestConnectClient(func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		cur *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		sc, err := lib.TableCursorToSerializedCursor(&psdbconnect.TableCursor{Keyspace: cur.Keyspace, Shard: cur.Shard, Position: "MySQL56/old:1-15"})
		assert.NoError(t, err)
		return sc, lib.ErrReadStoppedEarly
	})
	var scanned []string
	db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(map[string]lib.SchemaHistory{"-": trackedDDL}, nil, &scanned)
	assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("t")))
	assert.Equal(t, "MySQL56/old:1-15", lastPosition(t, tl.states, "SalesDB:t", "-"))
	assert.Equal(t, saved, tl.states[len(tl.states)-1].Keyspaces["SalesDB"].Streams["SalesDB:t"].ColumnOrder)
}

// Review finding 4: vtgate finding no tablet for the saved position during
// the scan is handled like the same error from Read: the first sync marks the
// cursor and fails, the next one raises the expired-binlogs Task for that shard.
func TestColumnOrderChangeScanTabletNotFoundFailsOnceThenRaisesTask(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	notFound := errors.New("rpc error: code = Unknown desc = failed to find a PRIMARY tablet for VStream in SalesDB/- within the planetscale_operator_default cell(s): context has expired")
	for sync := 1; sync <= 2; sync++ {
		tl := historianTestLogger(historianLive)
		var readTables, scanned []string
		db := lib.NewTestConnectClient(recordingReadFn(&readTables))
		db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(nil, map[string]error{"-": notFound}, &scanned)
		err := (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander"))
		assert.Equal(t, []string{"b_bystander/-"}, readTables, "sync %d", sync)
		if sync == 1 {
			if assert.Error(t, err) {
				assert.Contains(t, err.Error(), "a re-sync Task is raised")
			}
			assert.Empty(t, tl.tasks)
			assert.True(t, state.Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].Shards["-"].HasSuspectedBinlogExpiration())
			continue
		}
		assert.NoError(t, err)
		if assert.Len(t, tl.tasks, 1) {
			assert.Contains(t, tl.tasks[0], `binary logs needed to resume table "a_add_after" in keyspace "SalesDB" (shard "-") have expired`)
		}
	}
}

// Review finding 5: a scan that cannot reach the current position in time
// raises the re-sync Task instead of failing every sync the same way.
func TestColumnOrderChangeScanTimeoutRaisesResyncTask(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	tl := historianTestLogger(historianLive)
	var readTables, scanned []string
	db := lib.NewTestConnectClient(recordingReadFn(&readTables))
	db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(nil, map[string]error{"-": errors.Wrap(lib.ErrSchemaHistoryScanTimeout, "context deadline exceeded")}, &scanned)
	assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
	assert.Equal(t, []string{"b_bystander/-"}, readTables)
	if assert.Len(t, tl.tasks, 1) {
		assert.Equal(t, columnOrderTaskForAddAfter, tl.tasks[0])
	}
}

func TestIsConnectionWideError(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{status.Error(codes.Unauthenticated, "invalid credentials"), true},
		{errors.Wrap(status.Error(codes.PermissionDenied, "denied"), "Unable to get latest cursor position"), true},
		{fmt.Errorf("%w: %w", lib.ErrSchemaHistoryPositionCheck, status.Error(codes.Unauthenticated, "invalid credentials")), true},
		{status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp: lookup aws.connect.psdb.cloud: no such host"`), true},
		{status.Error(codes.Unavailable, "target: SalesDB.-.primary: vttablet: rpc error: code = Unavailable desc = connection error: desc = \"transport: ...\""), false},
		{status.Error(codes.Unavailable, "no healthy tablet available for 'keyspace:\"SalesDB\" shard:\"-\" tablet_type:PRIMARY'"), false},
		{status.Error(codes.DeadlineExceeded, "context deadline exceeded"), false},
		{errors.New("vttablet: rpc error: code = FailedPrecondition desc = column c not found in table t"), false},
	} {
		assert.Equal(t, tc.want, isConnectionWideError(tc.err), "%v", tc.err)
	}
}

// tabletNotFoundForTest is vtgate's tablet picker giving up, which Read and
// the scan report as suspected binlog expiration when it follows a
// successful position check.
var tabletNotFoundForTest = errors.New("rpc error: code = Unknown desc = failed to find a PRIMARY tablet for VStream in SalesDB/- within the planetscale_operator_default cell(s): context has expired")

// Second review, finding 3: once the sync's context is done, every remaining
// table would fail the same way. The sync stops at once, and an error from the
// cut-short Read is not classified (no suspected-expiry mark, no Task).
func TestCancelledSyncStopsWithoutMarkingOrTask(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	tl := historianTestLogger(historianSaved)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var readTables []string
	db := lib.NewTestConnectClient(func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		cur *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		readTables = append(readTables, tableName+"/"+cur.Shard)
		cancel()
		return nil, &lib.SuspectedBinlogExpirationError{Keyspace: cur.Keyspace, Shard: cur.Shard, Table: tableName, Err: tabletNotFoundForTest}
	})
	err := (&Sync{}).Handle(ctx, psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander"))
	assert.Equal(t, codes.Canceled, status.Code(err))
	assert.Equal(t, []string{"a_add_after/-"}, readTables)
	assert.Empty(t, tl.tasks)
	assert.False(t, state.Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].Shards["-"].HasSuspectedBinlogExpiration())
}

// Second review, finding 3 (scan path): a schema history scan cut short by
// the cancelled sync stops the sync too.
func TestCancelledSyncDuringSchemaHistoryScanStops(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	tl := historianTestLogger(historianLive)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var readTables []string
	db := lib.NewTestConnectClient(recordingReadFn(&readTables))
	db.(*lib.TestConnectClient).ScanSchemaHistoryFn = func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, tc *psdbconnect.TableCursor, resume *lib.SchemaScanProgress) (lib.SchemaHistory, error) {
		cancel()
		return lib.SchemaHistory{}, errors.Wrap(lib.ErrSchemaHistoryInconclusive, context.Canceled.Error())
	}
	err := (&Sync{}).Handle(ctx, psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander"))
	assert.Equal(t, codes.Canceled, status.Code(err))
	assert.Empty(t, readTables)
	assert.Empty(t, tl.tasks)
}

// outputFailingLogger fails every record send the way schemaAwareSerializer
// does when Fivetran's output stream is broken.
type outputFailingLogger struct {
	*orderedTestLogger
}

func (l *outputFailingLogger) Record(*sqltypes.Result, *fivetransdk.SchemaSelection, *fivetransdk.TableSelection, lib.Operation) error {
	return &OutputStreamError{Err: status.Error(codes.Unavailable, "transport is closing")}
}

// Second review, finding 4: a failed send to Fivetran ends the sync; the
// remaining tables are not streamed into a broken stream.
func TestOutputStreamFailureEndsTheSync(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	tl := &outputFailingLogger{historianTestLogger(historianSaved)}
	var readTables []string
	db := lib.NewTestConnectClient(func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string,
		cur *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor, onUpdate lib.OnUpdate,
	) (*lib.SerializedCursor, error) {
		readTables = append(readTables, tableName+"/"+cur.Shard)
		if err := onResult(sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"), lib.OpType_Insert); err != nil {
			// What lib.Read does with a callback error (rowCallbackError).
			return nil, fmt.Errorf("unable to serialize row: %w", err)
		}
		return nil, nil
	})
	err := (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander"))
	var ose *OutputStreamError
	assert.True(t, errors.As(err, &ose), "got %v", err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, []string{"a_add_after/-"}, readTables)
	assert.Empty(t, tl.tasks)
}

func TestSchemaAwareSerializerSendFailureIsOutputStreamError(t *testing.T) {
	sender := &testLogSender{sendError: status.Error(codes.Unavailable, "transport is closing")}
	l := NewSchemaAwareSerializer(sender, "test", false, nil, nil)
	for name, err := range map[string]error{
		"State":            l.State(lib.SyncState{}),
		"SendTask":         l.SendTask("task"),
		"SendWarningAlert": l.SendWarningAlert("warning"),
		"Truncate":         l.Truncate(&fivetransdk.SchemaSelection{SchemaName: "ks"}, &fivetransdk.TableSelection{TableName: "t"}),
	} {
		var ose *OutputStreamError
		assert.True(t, errors.As(err, &ose), "%s: got %v", name, err)
		assert.Equal(t, codes.Unavailable, status.Code(err), name)
	}
}

// Second review, finding 5: when the scan cannot even read the current
// position (no tablet, e.g. during a long reparent), that says nothing about
// the saved position: a plain error on every sync, never the expired-binlogs
// Task.
func TestColumnOrderChangeScanPositionCheckFailureIsNeverSuspectedExpiry(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	peekFailed := fmt.Errorf("%w: %w", lib.ErrSchemaHistoryPositionCheck, tabletNotFoundForTest)
	for sync := 1; sync <= 3; sync++ {
		tl := historianTestLogger(historianLive)
		var readTables, scanned []string
		db := lib.NewTestConnectClient(recordingReadFn(&readTables))
		db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(nil, map[string]error{"-": peekFailed}, &scanned)
		err := (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander"))
		assert.Error(t, err, "sync %d", sync)
		assert.NotContains(t, err.Error(), "a re-sync Task is raised", "sync %d", sync)
		assert.Empty(t, tl.tasks, "sync %d", sync)
		assert.Equal(t, []string{"b_bystander/-"}, readTables, "sync %d", sync)
		assert.False(t, state.Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].Shards["-"].HasSuspectedBinlogExpiration(), "sync %d", sync)
	}
}

// Second review, finding 7: the live order is read again when the sync gets
// to the table, not taken from the schema read at the start of the sync. A
// schema change in between is checked (here: the column added in the middle
// after the sync started), and the order saved is the fresh one.
func TestColumnOrderReadAgainWhenTheSyncGetsToTheTable(t *testing.T) {
	for name, tc := range map[string]struct {
		verdict   lib.SchemaHistory
		wantRead  bool
		wantSaved []string
	}{
		"tracked":   {trackedDDL, true, historianLive},
		"untracked": {untrackedDDL, false, historianSaved},
	} {
		t.Run(name, func(t *testing.T) {
			psc := &lib.PlanetScaleSource{}
			state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
			// The schema read at the start of the sync still has the saved order.
			tl := historianTestLogger(historianSaved)
			var readTables, scanned []string
			db := lib.NewTestConnectClient(recordingReadFn(&readTables))
			db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(map[string]lib.SchemaHistory{"-": tc.verdict}, nil, &scanned)
			db.(*lib.TestConnectClient).ColumnOrderFn = func(ctx context.Context, keyspace, table string) ([]string, error) {
				if table == "a_add_after" {
					return historianLive, nil
				}
				return []string{"id", "b"}, nil
			}
			assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
			assert.Equal(t, []string{"a_add_after/-@MySQL56/old:1-10"}, scanned)
			assert.Equal(t, tc.wantRead, slices.Contains(readTables, "a_add_after/-"))
			assert.Equal(t, tc.wantSaved, state.Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].ColumnOrder)
			if !tc.wantRead {
				assert.Len(t, tl.tasks, 1)
			}
		})
	}
}

// If the fresh read fails, the order from the start of the sync is used (it
// is older, so never ahead of the cursors).
func TestColumnOrderFallsBackToSourceSchemaWhenFreshReadFails(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	tl := historianTestLogger(historianSaved)
	var readTables []string
	db := lib.NewTestConnectClient(recordingReadFn(&readTables))
	db.(*lib.TestConnectClient).ColumnOrderFn = func(ctx context.Context, keyspace, table string) ([]string, error) {
		return nil, errors.New("information_schema unavailable")
	}
	assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
	assert.Equal(t, []string{"a_add_after/-", "b_bystander/-"}, readTables)
	assert.Equal(t, historianSaved, state.Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].ColumnOrder)
	assert.Contains(t, strings.Join(tl.infos, "\n"), "could not read the live column order")
}

// shardOrders answers shard-targeted column order reads from a map
// ("table/shard" -> order); a missing entry fails like an unusable target.
func shardOrders(orders map[string][]string, calls *[]string) lib.ShardColumnOrderFunc {
	return func(ctx context.Context, ps lib.PlanetScaleSource, keyspace, shard, table string) ([]string, error) {
		if calls != nil {
			*calls = append(*calls, table+"/"+shard)
		}
		if order, ok := orders[table+"/"+shard]; ok {
			return order, nil
		}
		return nil, errors.New("shard target not available")
	}
}

func shardedHistorianClient(readTables, scanned *[]string, verdicts map[string]lib.SchemaHistory, vtgate []string, perShard map[string][]string) lib.ConnectClient {
	db := lib.NewTestConnectClient(recordingReadFn(readTables))
	tcc := db.(*lib.TestConnectClient)
	tcc.ScanSchemaHistoryFn = scanByShard(verdicts, nil, scanned)
	tcc.ColumnOrderFn = func(ctx context.Context, keyspace, table string) ([]string, error) {
		if table == "a_add_after" {
			return vtgate, nil
		}
		return []string{"id", "b"}, nil
	}
	tcc.ShardColumnOrderFn = shardOrders(perShard, nil)
	return db
}

func savedShardOrders(state *lib.SyncState) (map[string][]string, []string) {
	ss := state.Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"]
	return ss.ShardColumnOrders, ss.ColumnOrder
}

// Review item 2 (per-shard column order): Online DDL cuts the shards over
// independently. Shard -80 cut over (column added in the middle, tracked),
// 80- not yet: only -80 is scanned, both shards are read, and each keeps its
// own order. When 80- cuts over later, its own saved order catches it: an
// untracked cut-over raises the tracker Task, a tracked one syncs and the
// table-level order follows once both shards agree.
func TestShardsCuttingOverAtDifferentTimes(t *testing.T) {
	for name, tc := range map[string]struct {
		lastShard lib.SchemaHistory
		wantTask  bool
	}{
		"last shard untracked": {untrackedDDL, true},
		"last shard tracked":   {trackedDDL, false},
	} {
		t.Run(name, func(t *testing.T) {
			psc := &lib.PlanetScaleSource{}
			state := historianTestState(t, historianSaved, map[string]string{"-80": "MySQL56/s1:1-10", "80-": "MySQL56/s2:1-10"})

			var readTables, scanned []string
			db := shardedHistorianClient(&readTables, &scanned, map[string]lib.SchemaHistory{"-80": trackedDDL}, historianLive,
				map[string][]string{"a_add_after/-80": historianLive, "a_add_after/80-": historianSaved})
			tl := historianTestLogger(historianLive)
			assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
			assert.Empty(t, tl.tasks)
			assert.Equal(t, []string{"a_add_after/-80@MySQL56/s1:1-10"}, scanned, "only the shard that cut over is scanned")
			slices.Sort(readTables)
			assert.Equal(t, []string{"a_add_after/-80", "a_add_after/80-", "b_bystander/-"}, readTables)
			perShard, tableLevel := savedShardOrders(state)
			assert.Equal(t, map[string][]string{"-80": historianLive, "80-": historianSaved}, perShard)
			assert.Equal(t, historianSaved, tableLevel, "the shards disagree: the table-level order stays behind")

			// 80- cuts over.
			readTables, scanned = nil, nil
			db = shardedHistorianClient(&readTables, &scanned, map[string]lib.SchemaHistory{"80-": tc.lastShard}, historianLive,
				map[string][]string{"a_add_after/-80": historianLive, "a_add_after/80-": historianLive})
			tl = historianTestLogger(historianLive)
			assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
			assert.Equal(t, []string{"a_add_after/80-@MySQL56/a_add_after:1-20"}, scanned, "-80's saved order matches its live order")
			perShard, tableLevel = savedShardOrders(state)
			if tc.wantTask {
				assert.Equal(t, []string{"b_bystander/-"}, readTables)
				if assert.Len(t, tl.tasks, 1) {
					assert.Equal(t, columnOrderTrackerTaskForAddAfter, tl.tasks[0])
				}
				assert.Equal(t, map[string][]string{"-80": historianLive, "80-": historianSaved}, perShard)
				assert.Equal(t, historianSaved, tableLevel)
				return
			}
			assert.Empty(t, tl.tasks)
			slices.Sort(readTables)
			assert.Equal(t, []string{"a_add_after/-80", "a_add_after/80-", "b_bystander/-"}, readTables)
			assert.Equal(t, map[string][]string{"-80": historianLive, "80-": historianLive}, perShard)
			assert.Equal(t, historianLive, tableLevel)
		})
	}
}

// Old state (table-level order only) is compared shard by shard with that
// order; the sync then saves every shard's own entry.
func TestOldStateTableLevelOrderFallback(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-80": "MySQL56/s1:1-10", "80-": "MySQL56/s2:1-10"})
	var readTables, scanned []string
	db := shardedHistorianClient(&readTables, &scanned, nil, historianSaved,
		map[string][]string{"a_add_after/-80": historianSaved, "a_add_after/80-": historianSaved})
	tl := historianTestLogger(historianSaved)
	assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
	assert.Empty(t, tl.tasks)
	assert.Empty(t, scanned)
	assert.Len(t, readTables, 3)
	perShard, tableLevel := savedShardOrders(state)
	assert.Equal(t, map[string][]string{"-80": historianSaved, "80-": historianSaved}, perShard)
	assert.Equal(t, historianSaved, tableLevel)
}

// A shard whose live order cannot be read (shard target not usable) is never
// passed silently: it is scanned, and its order is not saved. A shard that
// dropped a column is a change like any other: scanned, and once that passes
// its new order is saved. Without a scanner both get the re-sync Task.
func TestUnknownShardColumnOrderIsCheckedNotPassed(t *testing.T) {
	for name, tc := range map[string]struct {
		perShard  map[string][]string
		wantSaved []string
	}{
		"shard target fails":     {map[string][]string{"a_add_after/-80": historianSaved}, historianSaved},
		"shard dropped a column": {map[string][]string{"a_add_after/-80": historianSaved, "a_add_after/80-": {"id", "a", "b"}}, []string{"id", "a", "b"}},
	} {
		t.Run(name, func(t *testing.T) {
			perShard := tc.perShard
			psc := &lib.PlanetScaleSource{}
			state := historianTestState(t, historianSaved, map[string]string{"-80": "MySQL56/s1:1-10", "80-": "MySQL56/s2:1-10"})
			var readTables, scanned []string
			db := shardedHistorianClient(&readTables, &scanned, map[string]lib.SchemaHistory{"80-": noDDL}, historianSaved, perShard)
			tl := historianTestLogger(historianSaved)
			assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
			assert.Empty(t, tl.tasks)
			assert.Equal(t, []string{"a_add_after/80-@MySQL56/s2:1-10"}, scanned)
			assert.Len(t, readTables, 3)
			perShardSaved, _ := savedShardOrders(state)
			assert.Equal(t, tc.wantSaved, perShardSaved["80-"], "an unknown live order is never saved")

			// No schema history scan available: the re-sync Task, not a pass.
			state = historianTestState(t, historianSaved, map[string]string{"-80": "MySQL56/s1:1-10", "80-": "MySQL56/s2:1-10"})
			readTables = nil
			db.(*lib.TestConnectClient).ScanSchemaHistoryFn = nil
			tl = historianTestLogger(historianSaved)
			assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
			assert.Equal(t, []string{"b_bystander/-"}, readTables)
			if assert.Len(t, tl.tasks, 1) {
				assert.Equal(t, columnOrderTaskForAddAfter, tl.tasks[0])
			}
		})
	}
}

// Unsharded keyspace (shard "-"): vtgate's order is the shard's, as before;
// no shard-targeted query, one entry, and the table-level order follows it.
func TestUnshardedColumnOrderSingleEntry(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	appended := append(slices.Clone(historianSaved), "d")
	var readTables, scanned, shardCalls []string
	db := shardedHistorianClient(&readTables, &scanned, map[string]lib.SchemaHistory{"-": trackedDDL}, appended, nil)
	db.(*lib.TestConnectClient).ShardColumnOrderFn = shardOrders(nil, &shardCalls)
	tl := historianTestLogger(historianSaved)
	assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
	assert.Empty(t, tl.tasks)
	assert.Equal(t, []string{"a_add_after/-@MySQL56/old:1-10"}, scanned)
	assert.Empty(t, shardCalls)
	assert.Equal(t, []string{"a_add_after/-", "b_bystander/-"}, readTables)
	perShard, tableLevel := savedShardOrders(state)
	assert.Equal(t, map[string][]string{"-": appended}, perShard)
	assert.Equal(t, appended, tableLevel)
}

// Any column change without the schema tracker raises the enable-tracking
// Task, a column added at the end and a rename included (vttablet decodes the
// older events by position against the current schema); with the tracker
// the table syncs and the new order is saved.
func TestAnyColumnChangeNeedsTheSchemaTracker(t *testing.T) {
	for name, live := range map[string][]string{
		"added at end":     {"id", "a", "b", "c", "d"},
		"renamed in place": {"id", "a", "b2", "c"},
		"dropped":          {"id", "a", "c"},
	} {
		for _, tracked := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/tracked=%v", name, tracked), func(t *testing.T) {
				psc := &lib.PlanetScaleSource{}
				state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
				tl := historianTestLogger(live)
				var readTables, scanned []string
				db := lib.NewTestConnectClient(recordingReadFn(&readTables))
				verdict := untrackedDDL
				if tracked {
					verdict = trackedDDL
				}
				db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(map[string]lib.SchemaHistory{"-": verdict}, nil, &scanned)
				assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
				assert.Equal(t, []string{"a_add_after/-@MySQL56/old:1-10"}, scanned)
				order := state.Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].ColumnOrder
				if !tracked {
					assert.Equal(t, []string{"b_bystander/-"}, readTables)
					if assert.Len(t, tl.tasks, 1) {
						assert.Equal(t, columnOrderTrackerTaskForAddAfter, tl.tasks[0])
					}
					assert.Equal(t, historianSaved, order)
					return
				}
				assert.Empty(t, tl.tasks)
				assert.Equal(t, []string{"a_add_after/-", "b_bystander/-"}, readTables)
				assert.Equal(t, live, order)
			})
		}
	}
}

// Round 3 item 7: a sync that ends early with an error (here: a table with
// no stream state) still sends the Task an earlier table raised.
func TestPendingTaskSentWhenTheSyncEndsEarly(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	tl := historianTestLogger(historianLive)
	var readTables, scanned []string
	db := lib.NewTestConnectClient(recordingReadFn(&readTables))
	db.(*lib.TestConnectClient).ScanSchemaHistoryFn = scanByShard(map[string]lib.SchemaHistory{"-": untrackedDDL}, nil, &scanned)
	err := (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "c_without_state"))
	assert.ErrorContains(t, err, "Unable to read state for stream SalesDB:c_without_state")
	if assert.Len(t, tl.tasks, 1) {
		assert.Equal(t, columnOrderTrackerTaskForAddAfter, tl.tasks[0])
	}
}

// Round 3 item 8: a schema history scan that runs out of time after some
// progress saves that progress on the shard's cursor and checkpoints it; the
// table is skipped with a plain error (no Task), and the next sync resumes the
// scan from there. A scan that got nowhere still raises the Task (see
// TestColumnOrderChangeScanTimeoutRaisesResyncTask).
func TestColumnOrderChangeScanTimeoutResumesNextSync(t *testing.T) {
	psc := &lib.PlanetScaleSource{}
	state := historianTestState(t, historianSaved, map[string]string{"-": "MySQL56/old:1-10"})
	progress := &lib.SchemaScanProgress{Position: "MySQL56/old:1-14", PendingDDLs: []int64{1_700_000_000}, DDL: "RENAME TABLE ..."}
	var readTables []string
	var resumed []*lib.SchemaScanProgress
	db := lib.NewTestConnectClient(recordingReadFn(&readTables))
	db.(*lib.TestConnectClient).ScanSchemaHistoryFn = func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, tc *psdbconnect.TableCursor, resume *lib.SchemaScanProgress) (lib.SchemaHistory, error) {
		resumed = append(resumed, resume)
		if resume == nil {
			return lib.SchemaHistory{DDL: progress.DDL, Progress: progress}, errors.Wrap(lib.ErrSchemaHistoryScanTimeout, "context deadline exceeded")
		}
		return lib.SchemaHistory{Verdict: lib.SchemaHistoryTracked, DDL: progress.DDL, Progress: &lib.SchemaScanProgress{Position: "MySQL56/old:1-20", Tracked: true}}, nil
	}

	tl := historianTestLogger(historianLive)
	err := (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander"))
	assert.ErrorContains(t, err, "has not reached the current binlog position yet")
	assert.Empty(t, tl.tasks)
	assert.Equal(t, []string{"b_bystander/-"}, readTables)
	if assert.NotEmpty(t, tl.states) {
		saved := tl.states[len(tl.states)-1].Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].Shards["-"]
		assert.Equal(t, progress, saved.SchemaScan)
		assert.Equal(t, "MySQL56/old:1-10", lastPosition(t, tl.states, "SalesDB:a_add_after", "-"), "the cursor itself is kept")
	}

	// The next sync resumes the scan, which finishes: the table syncs, and
	// the new cursor carries no scan progress.
	readTables = nil
	tl = historianTestLogger(historianLive)
	assert.NoError(t, (&Sync{}).Handle(context.Background(), psc, &db, tl, state, columnOrderSelection("a_add_after", "b_bystander")))
	assert.Empty(t, tl.tasks)
	assert.Equal(t, []*lib.SchemaScanProgress{nil, progress}, resumed)
	assert.Equal(t, []string{"a_add_after/-", "b_bystander/-"}, readTables)
	assert.Nil(t, state.Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].Shards["-"].SchemaScan)
	assert.Equal(t, historianLive, state.Keyspaces["SalesDB"].Streams["SalesDB:a_add_after"].ColumnOrder)
}
