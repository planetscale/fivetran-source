package handlers

import (
	"context"
	"fmt"

	"github.com/planetscale/fivetran-source/lib"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/sqltypes"
)

type Sync struct{}

func (s *Sync) Handle(ctx context.Context, psc *lib.PlanetScaleSource, db *lib.ConnectClient, logger Serializer, state *lib.SyncState, schema *fivetransdk.Selection_WithSchema) error {
	err := s.handle(ctx, psc, db, logger, state, schema)
	// Send at most one combined Warning for every table/shard whose cursor was
	// reset because binlogs expired during this sync (no-op if none, or if it
	// was already flushed before raising a Task).
	if fErr := logger.FlushBinlogExpirationWarning(); fErr != nil {
		logger.Severe(fmt.Sprintf("failed to send binlog expiration warning: %v", fErr))
	}
	return err
}

func (s *Sync) handle(ctx context.Context, psc *lib.PlanetScaleSource, db *lib.ConnectClient, logger Serializer, state *lib.SyncState, schema *fivetransdk.Selection_WithSchema) error {
	if state == nil {
		return status.Error(codes.Internal, "syncState cannot be nil")
	}

	if db == nil {
		return status.Error(codes.Internal, "database accessor has not been initialized")
	}
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

			// First pass: check if all shards are at the initial cursor.
			allShardsHaveNoProgress := true
			hasShards := false

			for _, shardState := range streamState.Shards {
				hasShards = true
				tc, err := shardState.SerializedCursorToTableCursor()
				if err != nil {
					return status.Error(codes.Internal, fmt.Sprintf("invalid cursor for stream %v, failed with [%v]", stateKey, err))
				}

				// LastKnownPk is durable historical-copy progress even when Position is empty.
				if tc.Position != "" || tc.LastKnownPk != nil {
					allShardsHaveNoProgress = false
					break // No need to check remaining shards
				}
			}

			// Call truncate before any Read operations only when no shard has durable progress.
			if hasShards && allShardsHaveNoProgress {
				if err := logger.Truncate(ks, table); err != nil {
					return status.Error(codes.Internal, fmt.Sprintf("failed to truncate table %s, error: %s", table.TableName, err.Error()))
				}
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
					if task := resyncTaskMessage(err, ks.SchemaName, table.TableName, shardName); task != "" {
						// Checkpoint whatever progress the other shards/tables made, then
						// raise a single Task. Fivetran stops the sync on a Task.
						if sErr := logger.State(*state); sErr != nil {
							return status.Error(codes.Internal, sErr.Error())
						}
						logger.Severe(fmt.Sprintf("raising re-sync task for %s: %v", stateKey, err))
						// Warnings must go out before the Task: Fivetran stops the sync on a Task.
						if fErr := logger.FlushBinlogExpirationWarning(); fErr != nil {
							logger.Severe(fmt.Sprintf("failed to send binlog expiration warning: %v", fErr))
						}
						if tErr := logger.SendTask(task); tErr != nil {
							return status.Error(codes.FailedPrecondition, task)
						}
						return nil
					}
					return status.Error(codes.Internal, fmt.Sprintf("failed to download rows for table : %s , error : %s", table.TableName, err.Error()))
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

// resyncTaskMessage returns the customer-facing Fivetran Task text for read
// errors that cannot be recovered without a historical re-sync, or "" if the
// error should keep the existing retry/error behaviour.
func resyncTaskMessage(err error, keyspace, table, shard string) string {
	switch {
	// The PlanetScale connector only supports connection-level re-syncs in the
	// Fivetran dashboard (https://fivetran.com/docs/connectors/databases/planetscale),
	// so every Task points the customer at "Resync all data".
	case lib.IsShardNotFoundInVGTIDError(err):
		// Cursors are stored per table *and* per shard, so a removed shard affects
		// every table in the keyspace, not just the one that failed first.
		return fmt.Sprintf("Re-sync required: shard %q no longer exists in PlanetScale keyspace %q (for example after a reshard), "+
			"so incremental replication cannot resume for the tables in this keyspace. "+
			"To fix this, go to the connection's Settings tab in Fivetran and click \"Resync all data\".",
			shard, keyspace)
	case lib.IsVStreamSchemaIncompatibilityError(err):
		return fmt.Sprintf("Re-sync required: a schema change on table %q in PlanetScale keyspace %q (for example a column added in the middle "+
			"of the table, or a dropped or renamed column) prevents incremental replication from continuing. "+
			"To fix this, go to the connection's Settings tab in Fivetran and click \"Resync all data\".",
			table, keyspace)
	default:
		return ""
	}
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
