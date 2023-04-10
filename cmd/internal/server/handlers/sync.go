package handlers

import (
	"context"
	"fmt"

	"github.com/planetscale/fivetran-source/lib"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-proto/proto/fivetransdk/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/sqltypes"
)

type Sync struct{}

func (s *Sync) Handle(psc *lib.PlanetScaleSource, db *lib.PlanetScaleDatabase, logger Logger, state *lib.SyncState, schema *fivetransdk.Selection_WithSchema) error {
	if state == nil {
		return status.Error(codes.Internal, "syncState cannot be nil")
	}

	if db == nil {
		return status.Error(codes.Internal, "database accessor has not been initialized")
	}
	ctx := context.Background()
	for _, ks := range includedKeyspaces(schema) {
		for _, table := range includedTables(ks) {

			stateKey := ks.SchemaName + ":" + table.TableName
			streamState, ok := state.Keyspaces[ks.SchemaName].Streams[stateKey]
			if !ok {
				return status.Error(codes.Internal, fmt.Sprintf("Unable to read state for stream %v", stateKey))
			}
			onRow := func(res *sqltypes.Result) error {
				return logger.Record(res, ks, table)
			}

			for shardName, shardState := range streamState.Shards {
				onCursor := func(cursor *psdbconnect.TableCursor) error {
					sc, err := lib.TableCursorToSerializedCursor(cursor)
					if err != nil {
						return status.Error(codes.Internal, "unable to serialize table cursor")
					}
					state.Keyspaces[ks.SchemaName].Streams[stateKey].Shards[shardName] = sc
					return logger.State(*state)
				}
				tc, err := shardState.SerializedCursorToTableCursor()
				if err != nil {
					return status.Error(codes.Internal, fmt.Sprintf("invalid cursor for stream %v, failed with [%v]", stateKey, err))
				}
				sc, err := (*db).Read(ctx, logger, *psc, table.TableName, tc, onRow, onCursor)
				if err != nil {
					return status.Error(codes.Internal, fmt.Sprintf("failed to download rows for table : %s", table.TableName))
				}
				if sc != nil {
					// if we get any new state, we assign it here.
					// otherwise, the older state is round-tripped back to Fivetran.
					state.Keyspaces[ks.SchemaName].Streams[stateKey].Shards[shardName] = sc
				}
			}
		}
	}
	return logger.State(*state)
}

func includedKeyspaces(schema *fivetransdk.Selection_WithSchema) []*fivetransdk.SchemaSelection {
	var ks []*fivetransdk.SchemaSelection
	for _, keyspace := range schema.WithSchema.Schemas {
		if keyspace.Included {
			ks = append(ks, keyspace)
		}
	}

	return ks
}

func includedTables(keyspace *fivetransdk.SchemaSelection) []*fivetransdk.TableSelection {
	var ts []*fivetransdk.TableSelection
	for _, table := range keyspace.Tables {
		if table.Included {
			ts = append(ts, table)
		}
	}

	return ts
}
