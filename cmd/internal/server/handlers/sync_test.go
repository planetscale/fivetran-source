package handlers

import (
	"context"
	"testing"

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

func TestVStreamSchemaIncompatibilityReturnsFailedPrecondition(t *testing.T) {
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
	assert.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
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
