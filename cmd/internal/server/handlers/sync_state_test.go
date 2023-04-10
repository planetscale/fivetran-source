package handlers

import (
	"testing"

	fivetransdk "github.com/planetscale/fivetran-proto/go"
	"github.com/stretchr/testify/assert"
)

func TestCanInitializeState(t *testing.T) {
	schema := fivetransdk.Selection_WithSchema{
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
						{
							Included:  false,
							TableName: "customer_secrets",
						},
					},
				},
			},
		},
	}

	req := statefulRequest{}
	source := PlanetScaleSource{}
	state, err := StateFromRequest(req, source, []string{"-", "-40"}, schema)
	assert.NoError(t, err)
	assert.NotNil(t, state)
	ks, ok := state.Keyspaces["SalesDB"]
	assert.True(t, ok, "should fill in state for missing keyspace in stateJson")
	assert.NotNil(t, ks)
	_, ok = ks.Streams["SalesDB:customer_secrets"]
	assert.False(t, ok, "should not fill in state for skipped table in stateJson")

	table, ok := ks.Streams["SalesDB:customers"]
	assert.True(t, ok, "should fill in state for missing table in stateJson")
	assert.NotNil(t, table)
	shard, ok := table.Shards["-"]
	assert.True(t, ok, "should fill in state for missing shard \"-\" in stateJson")
	assert.NotNil(t, shard)
	assert.NotEmpty(t, shard.Cursor, "shard cursor should not be empty")

	shard, ok = table.Shards["-40"]
	assert.True(t, ok, "should fill in state for missing shard \"-40\" in stateJson")
	assert.NotNil(t, shard)
	assert.NotEmpty(t, shard.Cursor, "shard cursor should not be empty")
}

func TestCanFillInMissingState(t *testing.T) {
	schema := fivetransdk.Selection_WithSchema{
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
						{
							Included:  true,
							TableName: "regions",
						},
					},
				},
			},
		},
	}

	req := statefulRequest{
		stateJson: "{\"keyspaces\":{\"SalesDB\":{\"streams\":{\"SalesDB:customers\":{\"shards\":{\"-\":{\"cursor\":\"GhRUSElTX0lTX0FfVkFMSURfR1RJRA==\"}}}}}}}",
	}
	source := PlanetScaleSource{}
	state, err := StateFromRequest(req, source, []string{"-", "-40"}, schema)
	assert.NoError(t, err)
	assert.NotNil(t, state)
	ks, ok := state.Keyspaces["SalesDB"]
	assert.True(t, ok, "should fill in state for keyspace found in stateJson")
	assert.NotNil(t, ks)
	table, ok := ks.Streams["SalesDB:customers"]
	assert.True(t, ok, "should fill in state for table found in stateJson")
	assert.NotNil(t, table)
	shard, ok := table.Shards["-"]
	assert.True(t, ok, "should fill in state for shard \"-\" found in stateJson")
	assert.NotNil(t, shard)
	assert.Equal(t, "GhRUSElTX0lTX0FfVkFMSURfR1RJRA==", shard.Cursor, "shard cursor should not be empty")

	shard, ok = table.Shards["-40"]
	assert.True(t, ok, "should fill in state for missing shard \"-40\" in stateJson")
	assert.NotNil(t, shard)
	assert.NotEmpty(t, shard.Cursor, "shard cursor should not be empty")

	tableMissingInState, ok := ks.Streams["SalesDB:regions"]
	assert.True(t, ok, "should fill in state for table 'regions' missing in stateJson")
	assert.NotNil(t, table)
	shard, ok = tableMissingInState.Shards["-"]
	assert.True(t, ok, "should fill in state for shard \"-\" missing in stateJson")
	assert.NotNil(t, shard)
	assert.NotEmpty(t, shard.Cursor, "shard cursor should not be empty")

	shard, ok = tableMissingInState.Shards["-"]
	assert.True(t, ok, "should fill in state for missing shard \"-40\" in stateJson")
	assert.NotNil(t, shard)
	assert.NotEmpty(t, shard.Cursor, "shard cursor should not be empty")
}
