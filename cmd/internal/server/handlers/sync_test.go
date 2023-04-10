package handlers

import (
	"context"
	"testing"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-proto/go"
	"github.com/stretchr/testify/assert"
)

func TestCallsReadWithSelectedSchema(t *testing.T) {
	psc := &PlanetScaleSource{}
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

	readFn := func(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, table *fivetransdk.TableSelection, tc *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor) (*SerializedCursor, error) {
		assert.Equal(t, "customers", table.TableName)
		return nil, nil
	}

	db := NewTestConnectClient(readFn, blankDiscoverFn)
	err := sync.Handle(psc, &db, tl, &SyncState{
		Keyspaces: map[string]KeyspaceState{
			"SalesDB": {
				Streams: map[string]ShardStates{
					"SalesDB:customers": {},
				},
			},
		},
	}, schemaSelection)
	assert.NoError(t, err)
}
