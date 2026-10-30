package server

import (
	"testing"

	"github.com/planetscale/fivetran-source/cmd/internal/server/handlers"
	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"github.com/planetscale/fivetran-source/lib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type captureSender struct{ responses []*fivetransdk.UpdateResponse }

func (c *captureSender) Send(r *fivetransdk.UpdateResponse) error {
	c.responses = append(c.responses, r)
	return nil
}

func (c *captureSender) warnings() []string {
	var out []string
	for _, r := range c.responses {
		if w := r.GetWarning(); w != nil {
			out = append(out, w.GetMessage())
		}
	}
	return out
}

// Older connector versions stored shards of other keyspaces whose name
// contained the database name (e.g. "prod_main_sharded/-40" for "prod_main").
func TestStateFromRequestDropsInvalidLegacyShards(t *testing.T) {
	stateJSON := `{"keyspaces":{"prod_main":{"streams":{` +
		`"prod_main:states":{"shards":{"-":{"cursor":""},"prod_main_sharded/-40":{"cursor":""},"prod_main_sharded/40-80":{"cursor":""}}},` +
		`"prod_main:users":{"shards":{"-":{"cursor":""},"prod_main_sharded/-40":{"cursor":""}}}}}}}`
	req := &fivetransdk.UpdateRequest{StateJson: &stateJSON}
	selection := fivetransdk.Selection_WithSchema{WithSchema: &fivetransdk.TablesWithSchema{
		Schemas: []*fivetransdk.SchemaSelection{{
			SchemaName: "prod_main",
			Included:   true,
			Tables: []*fivetransdk.TableSelection{
				{TableName: "states", Included: true},
				{TableName: "users", Included: true},
			},
		}},
	}}
	sender := &captureSender{}
	logger := handlers.NewSchemaAwareSerializer(sender, "test", false, nil, nil)

	state, err := StateFromRequest(logger, req, lib.PlanetScaleSource{Database: "prod_main"}, []string{"-"}, selection)
	require.NoError(t, err)
	shards := state.Keyspaces["prod_main"].Streams["prod_main:states"].Shards
	assert.Contains(t, shards, "-")
	assert.NotContains(t, shards, "prod_main_sharded/-40")
	assert.NotContains(t, state.Keyspaces["prod_main"].Streams["prod_main:users"].Shards, "prod_main_sharded/-40")

	// Exactly one Warning for the whole sync, each stale shard listed once.
	warnings := sender.warnings()
	require.Len(t, warnings, 1)
	assert.Equal(t, "PlanetScale removed stale shard references left in this connection's sync state by an older connector version "+
		"(keyspace prod_main: prod_main_sharded/-40, prod_main_sharded/40-80). "+
		"These references never contained data. No action or re-sync is needed.", warnings[0])
}

func TestStateFromRequestNoWarningWithoutStaleShards(t *testing.T) {
	stateJSON := `{"keyspaces":{"prod_main":{"streams":{"prod_main:states":{"shards":{"-":{"cursor":""}}}}}}}`
	req := &fivetransdk.UpdateRequest{StateJson: &stateJSON}
	selection := fivetransdk.Selection_WithSchema{WithSchema: &fivetransdk.TablesWithSchema{
		Schemas: []*fivetransdk.SchemaSelection{{
			SchemaName: "prod_main",
			Included:   true,
			Tables:     []*fivetransdk.TableSelection{{TableName: "states", Included: true}},
		}},
	}}
	sender := &captureSender{}
	logger := handlers.NewSchemaAwareSerializer(sender, "test", false, nil, nil)

	_, err := StateFromRequest(logger, req, lib.PlanetScaleSource{Database: "prod_main"}, []string{"-"}, selection)
	require.NoError(t, err)
	assert.Empty(t, sender.warnings())
}
