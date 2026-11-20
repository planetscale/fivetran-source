package handlers

import (
	"context"
	"encoding/json"
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
