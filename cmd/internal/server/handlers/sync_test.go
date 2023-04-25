package handlers

import (
	"context"
	"testing"

	"github.com/planetscale/fivetran-source/lib"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-proto/go"
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

	readFn := func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, columns []string, tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor) (*lib.SerializedCursor, error) {
		assert.Equal(t, "customers", tableName)
		return nil, nil
	}

	db := lib.NewTestConnectClient(readFn)
	err := sync.Handle(psc, &db, tl, &lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			"SalesDB": {
				Streams: map[string]lib.ShardStates{
					"SalesDB:customers": {},
				},
			},
		},
	}, schemaSelection)
	assert.NoError(t, err)
}
