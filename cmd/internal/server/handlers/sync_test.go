package handlers

import (
	"context"
	"testing"

	"github.com/planetscale/fivetran-source/lib"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"github.com/stretchr/testify/assert"
)

func TestCallsReadWithSelectedSchema(t *testing.T) {
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
		assert.Equal(t, "customers", tableName)
		return nil, nil
	}

	state, err := psc.GetInitialState("SalesDB", []string{"-"})
	assert.NoError(t, err)
	db := lib.NewTestConnectClient(readFn)
	err = sync.Handle(psc, &db, tl, &lib.SyncState{
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
	err = sync.Handle(psc, &db, tl, &lib.SyncState{
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
	err = sync.Handle(psc, &db, tl, &lib.SyncState{
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
