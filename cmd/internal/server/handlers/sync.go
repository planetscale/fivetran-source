package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/planetscale/fivetran-source/lib"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/sqltypes"
)

type Sync struct{}

func (s *Sync) Handle(psc *lib.PlanetScaleSource, db *lib.ConnectClient, logger Serializer, state *lib.SyncState, schema *fivetransdk.Selection_WithSchema) error {
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
			onRow := func(res *sqltypes.Result, op lib.Operation) error {
				return logger.Record(res, ks, table, op)
			}

			onUpdate := func(upd *lib.UpdatedRow) error {
				return logger.Update(upd, ks, table)
			}

			// First pass: check if all shards have empty positions
			allShardsHaveEmptyPosition := true
			hasShards := false

			for _, shardState := range streamState.Shards {
				hasShards = true
				tc, err := shardState.SerializedCursorToTableCursor()
				if err != nil {
					return status.Error(codes.Internal, fmt.Sprintf("invalid cursor for stream %v, failed with [%v]", stateKey, err))
				}

				// Check if this shard has data (non-empty position)
				if tc.Position != "" {
					allShardsHaveEmptyPosition = false
					break // No need to check remaining shards
				}
			}

			// Call truncate before any Read operations if all shards are empty
			if hasShards && allShardsHaveEmptyPosition {
				logger.Truncate(ks, table)
			}

			// Second pass: perform the actual sync for each shard
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

				columns := includedColumns(table)
				sc, err := (*db).Read(ctx, logger, *psc, table.TableName, columns, tc, onRow, onCursor, onUpdate)
				if err != nil {
					if errors.Is(err, lib.BinLogsExpirationError) {
						logger.Info(fmt.Sprintf("Historical sync required for table %s, resetting cursor position", table.TableName))

						// Reset the cursor position to empty to trigger historical sync
						emptyCursor := &psdbconnect.TableCursor{
							Shard:    tc.Shard,
							Keyspace: tc.Keyspace,
							Position: "",
						}

						emptySerializedCursor, serErr := lib.TableCursorToSerializedCursor(emptyCursor)
						if serErr != nil {
							return status.Error(codes.Internal, fmt.Sprintf("failed to serialize empty cursor for historical sync: %s", serErr.Error()))
						}

						// Update the state with empty cursor
						state.Keyspaces[ks.SchemaName].Streams[stateKey].Shards[shardName] = emptySerializedCursor

						// Trigger truncate since we're doing historical sync
						logger.Truncate(ks, table)

						// Update tc to the empty cursor for the next iteration
						tc = emptyCursor

						// Retry the Read operation with the empty cursor
						sc, err = (*db).Read(ctx, logger, *psc, table.TableName, columns, tc, onRow, onCursor, onUpdate)
						if err != nil {
							return status.Error(codes.Internal, fmt.Sprintf("failed to download rows for table after historical sync reset: %s, error: %s", table.TableName, err.Error()))
						}
					} else {
						return status.Error(codes.Internal, fmt.Sprintf("failed to download rows for table : %s , error : %s", table.TableName, err.Error()))
					}
				}
				if sc != nil {
					// if we get any new state, we assign it here.
					// otherwise, the older state is round-tripped back to Fivetran.
					state.Keyspaces[ks.SchemaName].Streams[stateKey].Shards[shardName] = sc
					logger.State(*state) // Checkpoint after every shard
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

func includedColumns(table *fivetransdk.TableSelection) []string {
	var columns []string
	for column, included := range table.Columns {
		if included {
			columns = append(columns, column)
		}
	}
	return columns
}
