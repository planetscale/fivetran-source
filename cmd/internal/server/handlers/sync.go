package handlers

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/pkg/errors"

	"github.com/planetscale/fivetran-source/lib"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/sqltypes"
)

type Sync struct{}

func (s *Sync) Handle(ctx context.Context, psc *lib.PlanetScaleSource, db *lib.ConnectClient, logger Serializer, state *lib.SyncState, schema *fivetransdk.Selection_WithSchema) error {
	if err := s.handle(ctx, psc, db, logger, state, schema); err != nil {
		// Don't tell the customer "No action is needed" on a failing sync. The
		// historical re-sync still recovers on the next sync: either the reset
		// cursor was already checkpointed, or the binlogs are still expired and
		// the cursor is reset (and recorded) again.
		return err
	}
	// Send at most one combined Warning for every table/shard whose cursor was
	// reset because binlogs expired during this sync (no-op if none, or if it
	// was already flushed before raising a Task).
	if fErr := logger.FlushBinlogExpirationWarning(); fErr != nil {
		logger.Severe(fmt.Sprintf("failed to send binlog expiration warning: %v", fErr))
	}
	return nil
}

func (s *Sync) handle(ctx context.Context, psc *lib.PlanetScaleSource, db *lib.ConnectClient, logger Serializer, state *lib.SyncState, schema *fivetransdk.Selection_WithSchema) (retErr error) {
	if state == nil {
		return status.Error(codes.Internal, "syncState cannot be nil")
	}

	if db == nil {
		return status.Error(codes.Internal, "database accessor has not been initialized")
	}
	// A table that fails must not hold up the other tables in the connection:
	// remember the first error/Task, keep syncing the remaining tables, and
	// report it after their progress has been checkpointed.
	var (
		firstErr    error
		pendingTask string
		// outputErr is the first failure to send to Fivetran's output stream
		// (see OutputStreamError). Nothing more can reach Fivetran after it,
		// so the sync ends instead of streaming the remaining tables.
		outputErr error
		// taskSent: pendingTask went out (at the end of the sync).
		taskSent bool
	)
	defer func() {
		// A sync that ends early with an error (a missing stream state, an
		// invalid cursor, a failed Truncate or checkpoint) still sends the
		// Task raised for an earlier table, so the customer is told what to
		// do, unless nothing more can reach Fivetran (its output stream broke
		// or the sync was cancelled).
		if retErr == nil || pendingTask == "" || taskSent || outputErr != nil || ctx.Err() != nil {
			return
		}
		if fErr := logger.FlushBinlogExpirationWarning(); fErr != nil {
			logger.Severe(fmt.Sprintf("failed to send binlog expiration warning: %v", fErr))
		}
		if tErr := logger.SendTask(pendingTask); tErr != nil {
			logger.Severe(fmt.Sprintf("failed to send the pending Task after the sync failed: %v", tErr))
		}
	}()
	noteOutput := func(err error) error {
		var ose *OutputStreamError
		if outputErr == nil && errors.As(err, &ose) {
			outputErr = err
		}
		return err
	}
	// stop reports why the sync must end now, or nil: the sync's context is
	// done (Fivetran cancelled the call or its deadline passed, so every
	// remaining table would fail the same way), or Fivetran's output stream
	// broke.
	stop := func(stateKey string) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			logger.Severe(fmt.Sprintf("stopping the sync before table %s: %v", stateKey, ctxErr))
			return status.FromContextError(ctxErr).Err()
		}
		if outputErr != nil {
			logger.Severe(fmt.Sprintf("stopping the sync at table %s: sending to Fivetran failed: %v", stateKey, outputErr))
			return outputErr
		}
		return nil
	}
keyspaces:
	for _, ks := range includedKeyspaces(schema) {
		for _, table := range includedTables(ks) {
			stateKey := ks.SchemaName + ":" + table.TableName
			if err := stop(stateKey); err != nil {
				return err
			}
			streamState, ok := state.Keyspaces[ks.SchemaName].Streams[stateKey]
			if !ok {
				return status.Error(codes.Internal, fmt.Sprintf("Unable to read state for stream %v", stateKey))
			}
			onRow := func(res *sqltypes.Result, op lib.Operation) error {
				return noteOutput(logger.Record(res, ks, table, op))
			}

			onUpdate := func(upd *lib.UpdatedRow) error {
				return noteOutput(logger.Update(upd, ks, table))
			}

			// Any change of the column order since the saved cursors (a column
			// added anywhere, dropped, renamed or moved): without the vttablet
			// schema historian the older binlog events would be decoded by
			// position against the current schema. Stream the table only if
			// the schema tracker recorded the change (or the cursors are
			// already past it); otherwise raise the re-sync Task instead of
			// streaming the table.
			// Every shard is checked against its own saved and live order: Online
			// DDL cuts the shards over independently.
			order := checkColumnOrder(ctx, db, logger, psc, ks.SchemaName, table.TableName, streamState)
			// trackedShards: the shards whose schema history scan in this sync
			// found every schema change tracked; stepOverColumns: per changed
			// shard, the selected new columns that may be stepped over if so
			// (see stepOverColumns, read before the scan).
			var (
				trackedShards   map[string]trackedRange
				stepOverColumns map[string][]string
			)
			if orderErr := order.changed; orderErr != nil {
				stepOverColumns = stepOverCandidates(ctx, db, logger, psc, ks.SchemaName, table.TableName, streamState, order, includedColumns(table))
				tracked, failedShard, hErr := verifySchemaHistory(ctx, db, logger, psc, streamState, orderErr, order)
				trackedShards = tracked
				if err := stop(stateKey); err != nil {
					// A scan cut short by the cancelled sync proves nothing about
					// the table: no mark, no Task.
					return err
				}
				if hErr == nil {
					why := "the saved cursors are already past the schema change"
					if len(tracked) > 0 {
						why = "the schema tracker recorded the schema change, so the older binlog events are decoded with the schema they were written with"
					}
					logger.Info(fmt.Sprintf("table %s: column order changed on shard(s) %s (%s); %s, syncing as usual",
						stateKey, strings.Join(order.changedShards, ","), orderErr.Reason, why))
				} else {
					var incomplete *schemaScanIncompleteError
					if errors.As(hErr, &incomplete) {
						// The scan saved how far it got on the shard's cursor:
						// checkpoint it so the next sync resumes there. The
						// table is not streamed until the scan is done.
						if sErr := noteOutput(logger.State(*state)); sErr != nil {
							return status.Error(codes.Internal, sErr.Error())
						}
						summary := logSkippedTable(logger, stateKey, hErr)
						if firstErr == nil {
							firstErr = status.Error(codes.Internal, fmt.Sprintf("failed to download rows for table : %s , error : %s", table.TableName, summary))
						}
						continue
					}
					var suspected *lib.SuspectedBinlogExpirationError
					if errors.As(hErr, &suspected) && !streamState.Shards[failedShard].HasSuspectedBinlogExpiration() {
						// Same as a Read that fails this way (see below): mark the
						// cursor and fail this sync; the next one raises the Task.
						streamState.Shards[failedShard].SetSuspectedBinlogExpiration()
						if sErr := noteOutput(logger.State(*state)); sErr != nil {
							return status.Error(codes.Internal, sErr.Error())
						}
						summary := logSkippedTable(logger, stateKey, hErr)
						if firstErr == nil {
							firstErr = status.Error(codes.Internal, fmt.Sprintf("failed to download rows for table : %s , error : %s "+
								"(if the next sync fails the same way, a re-sync Task is raised)", table.TableName, summary))
						}
						continue
					}
					summary := logSkippedTable(logger, stateKey, hErr)
					if task := resyncTaskMessage(hErr, ks.SchemaName, table.TableName, failedShard); task != "" {
						if pendingTask == "" {
							pendingTask = task
						}
					} else if firstErr == nil {
						firstErr = status.Error(codes.Internal, fmt.Sprintf("failed to download rows for table : %s , error : %s", table.TableName, summary))
					}
					if isConnectionWideError(hErr) {
						logger.Severe(fmt.Sprintf("stopping the sync after table %s: the error affects every table in the connection", stateKey))
						break keyspaces
					}
					continue
				}
			}
			// First pass: check if all shards are at the initial cursor. After
			// the column order check, whose schema history scan can reset a
			// cursor whose binary logs expired: a table whose every shard is
			// copied again must be truncated first, or rows deleted at the
			// source meanwhile would stay in the destination.
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
				if err := noteOutput(logger.Truncate(ks, table)); err != nil {
					return status.Error(codes.Internal, fmt.Sprintf("failed to truncate table %s, error: %s", table.TableName, err.Error()))
				}
			}

			// Second pass: perform the actual sync for each shard
			for shardName, shardState := range streamState.Shards {
				// The shard's live column order is saved once the shard's
				// cursor is known not to be behind it (see pendingColumnOrder).
				pending := newPendingColumnOrder(ctx, db, logger, psc, state, ks.SchemaName, table.TableName, shardName, order.shards[shardName])
				onCursor := func(cursor *psdbconnect.TableCursor) error {
					sc, err := lib.TableCursorToSerializedCursor(cursor)
					if err != nil {
						return status.Error(codes.Internal, "unable to serialize table cursor")
					}
					state.Keyspaces[ks.SchemaName].Streams[stateKey].Shards[shardName] = sc
					// Also during a copy that spans several syncs: the order
					// goes out with the first checkpoint that covers it.
					pending.saveIfCovered(state, cursor.Position)
					return noteOutput(logger.State(*state))
				}
				tc, err := shardState.SerializedCursorToTableCursor()
				if err != nil {
					return status.Error(codes.Internal, fmt.Sprintf("invalid cursor for stream %v, failed with [%v]", stateKey, err))
				}

				columns := includedColumns(table)
				readCtx := ctx
				if r, ok := trackedShards[shardName]; ok {
					if stepOver := trackedStepOver(logger, ks.SchemaName, table.TableName, shardName, r, stepOverColumns[shardName]); stepOver != nil {
						readCtx = lib.WithTrackedStepOver(ctx, *stepOver)
					}
				}
				sc, err := (*db).Read(readCtx, logger, *psc, table.TableName, columns, tc, onRow, onCursor, onUpdate)
				if sErr := stop(stateKey); sErr != nil {
					// Checked before the error is classified: a Read cut short by
					// the cancelled sync can fail with vtgate's "no tablet found",
					// which must not count as suspected binlog expiration.
					return sErr
				}
				readComplete := err == nil
				if errors.Is(err, lib.ErrReadStoppedEarly) {
					// Read gave up after repeated timeouts (it already sent a
					// Warning), or vtgate ended the stream early: keep the cursor
					// it reached, but the shard was not read up to the current
					// position, so its live column order is not saved.
					err = nil
				}
				var suspected *lib.SuspectedBinlogExpirationError
				if errors.As(err, &suspected) && !shardState.HasSuspectedBinlogExpiration() {
					// Seen once: a tablet that is briefly unavailable (for example during
					// a reparent) can look the same. Remember it on the cursor (Fivetran
					// returns the last checkpoint on the next sync, even after a failed
					// one) and fail this sync normally so Fivetran retries. If the next
					// sync fails the same way, resyncTaskMessage raises the Task.
					// The mark is its own field: it never touches error_code /
					// error_message, which belong to the explicit-expiry reset in lib.Read.
					// It goes on the shard's current cursor, which onCursor may have
					// replaced during this Read.
					state.Keyspaces[ks.SchemaName].Streams[stateKey].Shards[shardName].SetSuspectedBinlogExpiration()
					if sErr := noteOutput(logger.State(*state)); sErr != nil {
						return status.Error(codes.Internal, sErr.Error())
					}
					summary := logSkippedTable(logger, stateKey, err)
					if firstErr == nil {
						firstErr = status.Error(codes.Internal, fmt.Sprintf("failed to download rows for table : %s , error : %s "+
							"(if the next sync fails the same way, a re-sync Task is raised)", table.TableName, summary))
					}
					break
				}
				if err != nil {
					task := resyncTaskMessage(err, ks.SchemaName, table.TableName, shardName)
					if task == "" {
						task = serializationTaskMessage(err, ks.SchemaName, table.TableName, psc.TreatTinyIntAsBoolean)
						var rse *RowSerializationError
						if task != "" && errors.As(err, &rse) {
							// The primary key goes to the connector log only, not the Task.
							logger.Severe(serializationLogMessage(rse, ks.SchemaName, table.TableName))
						}
					}
					summary := logSkippedTable(logger, stateKey, err)
					if task != "" {
						if pendingTask == "" {
							pendingTask = task
						}
					} else if firstErr == nil {
						firstErr = status.Error(codes.Internal, fmt.Sprintf("failed to download rows for table : %s , error : %s", table.TableName, summary))
					}
					if isConnectionWideError(err) {
						logger.Severe(fmt.Sprintf("stopping the sync after table %s: the error affects every table in the connection", stateKey))
						break keyspaces
					}
					break
				}
				if sc != nil {
					// if we get any new state, we assign it here.
					// otherwise, the older state is round-tripped back to Fivetran.
					state.Keyspaces[ks.SchemaName].Streams[stateKey].Shards[shardName] = sc
					if end, cErr := sc.SerializedCursorToTableCursor(); cErr == nil {
						pending.saveIfCovered(state, end.Position)
					}
					if sErr := noteOutput(logger.State(*state)); sErr != nil { // Checkpoint after every shard
						return sErr
					}
				} else if shardState.HasSuspectedBinlogExpiration() {
					// The shard synced fine, so the earlier failure was transient.
					shardState.ClearSuspectedBinlogExpiration()
					if sErr := noteOutput(logger.State(*state)); sErr != nil {
						return sErr
					}
				}
				// Save the shard's live column order once the shard's cursor
				// contains the primary position read after that order, whatever
				// the other shards did (see pendingColumnOrder). An unknown live
				// order is never saved: the shard keeps its saved order and is
				// checked again.
				if cur, cErr := state.Keyspaces[ks.SchemaName].Streams[stateKey].Shards[shardName].SerializedCursorToTableCursor(); cErr == nil {
					pending.saveAfterRead(state, readComplete, cur.Position)
				}
			}
		}
	}
	if sErr := noteOutput(logger.State(*state)); sErr != nil {
		return status.Error(codes.Internal, sErr.Error())
	}
	if pendingTask != "" {
		taskSent = true
		// Fivetran stops the sync on a Task, so it goes out last, after every
		// other table was synced and checkpointed, and after any Warnings.
		if fErr := logger.FlushBinlogExpirationWarning(); fErr != nil {
			logger.Severe(fmt.Sprintf("failed to send binlog expiration warning: %v", fErr))
		}
		if tErr := logger.SendTask(pendingTask); tErr != nil {
			return status.Error(codes.FailedPrecondition, pendingTask)
		}
		return nil
	}
	return firstErr
}

// logSkippedTable logs why a table is skipped for the rest of the sync and
// returns the short form of err used in the sync error. Fivetran cuts long
// messages, and a VStream error leads with the GTID set of the saved position
// (kilobytes on a long-lived database), so the short line leads with the table
// and vttablet's reason (lib.VStreamErrorReason) and the full error, GTIDs
// included, goes in a line of its own.
func logSkippedTable(logger Serializer, stateKey string, err error) string {
	summary := lib.VStreamErrorReason(err)
	if summary == "" {
		summary = err.Error()
	}
	logger.Severe(fmt.Sprintf("skipping table %s for the rest of this sync: %s", stateKey, summary))
	if summary != err.Error() {
		logger.Severe(fmt.Sprintf("full error for table %s: %v", stateKey, err))
	}
	return summary
}

// ColumnOrderChangedError reports that a table's columns are no longer in the
// order saved with its cursors (see lib.ColumnOrderChange). It is only logged;
// the Task text comes from resyncTaskMessage and has no column names.
type ColumnOrderChangedError struct {
	Keyspace string
	Table    string
	Reason   string   // from lib.ColumnOrderChange
	Saved    []string // column order saved with the cursors
	Current  []string // live column order
	// Untracked is set when a shard's binlog has the schema change but no
	// schema version after it (lib.SchemaHistoryUntracked): the keyspace does
	// not run vttablet's schema tracker. Shard and DDL are from that shard.
	Untracked bool
	Shard     string
	DDL       string
}

func (e *ColumnOrderChangedError) Error() string {
	msg := fmt.Sprintf("column order of table %s changed since the last sync (%s), so binlog events from before the change "+
		"would be decoded into the wrong columns; saved order: %s; current order: %s",
		e.Table, e.Reason, strings.Join(e.Saved, ","), strings.Join(e.Current, ","))
	if e.Untracked {
		msg += fmt.Sprintf("; shard %s has the schema change %q with no schema version after it (track_schema_versions is off)", e.Shard, e.DDL)
	}
	return msg
}

// columnOrderCheck is the result of checkColumnOrder for one table.
type columnOrderCheck struct {
	// shards has every shard of the table; nil when the Serializer does not
	// know the source schema of the table (the check is skipped).
	shards map[string]*shardColumnOrder
	// changed is set for the first shard (in shard order) whose live order is
	// not its saved order (lib.ColumnOrderChange), or whose live order could
	// not be read.
	changed *ColumnOrderChangedError
	// changedShards lists every such shard, sorted; only these are scanned.
	changedShards []string
}

// shardColumnOrder is the column order check of one shard.
type shardColumnOrder struct {
	// live is the shard's live column order, saved for the shard once it was
	// read up to the current position; nil when it could not be read.
	live []string
}

// errUnknownShardColumnOrder is the Reason of a ColumnOrderChangedError for a
// shard whose live column order could not be read: it is checked like a
// changed order (schema history scan, or the Task), never passed silently.
const errUnknownShardColumnOrder = "the live column order of the shard could not be read"

// checkColumnOrder compares, for every shard of the table, the column order
// saved for that shard (lib.ShardStates.SavedColumnOrder: its own entry, or
// else the table-level order of older state) with the shard's own live order,
// if the Serializer knows the source schema (see SourceColumnOrderer).
//
// Online DDL cuts every shard over on its own, so vtgate's answer (from one of
// the shards) does not describe the others. For a table with more than one
// shard the live order of each shard is read with a shard-targeted query
// (lib.ShardColumnOrderReader, INVISIBLE columns included). If that fails the
// shard's live order is unknown: it is checked like a changed order (schema
// history scan or Task) and never saved.
// For an unsharded keyspace (one shard, "-") vtgate's order is that shard's,
// as before. vtgate's order is read again when the sync gets to the table
// (lib.ColumnOrderReader), else taken from the schema read at the start of the
// sync.
//
// Every live order is read before any of the shard's binlog positions, so the
// order saved after the shard's read is never ahead of its cursor. It never
// changes the saved orders: the caller saves a shard's live order once that
// shard's read completed, and keeps the saved orders when it raises the Task,
// so every sync raises it until the connection is re-synced (or
// verifySchemaHistory clears it).
//
// A shard with no durable progress (initial sync, "Resync all data", or a
// cursor reset after binlog expiration) is copied from scratch: it is not
// compared, only its order is recorded.
func checkColumnOrder(ctx context.Context, db *lib.ConnectClient, logger Serializer, psc *lib.PlanetScaleSource, keyspace, table string, streamState lib.ShardStates) columnOrderCheck {
	orderer, ok := logger.(SourceColumnOrderer)
	if !ok {
		return columnOrderCheck{}
	}
	current, ok := orderer.SourceColumnOrder(keyspace, table)
	if !ok {
		return columnOrderCheck{}
	}
	shards := make([]string, 0, len(streamState.Shards))
	for shard := range streamState.Shards {
		shards = append(shards, shard)
	}
	slices.Sort(shards)
	unsharded := len(shards) == 1 && shards[0] == "-"
	if reader, ok := (*db).(lib.ColumnOrderReader); ok && unsharded {
		fresh, err := reader.ColumnOrder(ctx, keyspace, table)
		switch {
		case err == nil && len(fresh) > 0:
			current = fresh
		case err != nil && !errors.Is(err, lib.ErrColumnOrderUnavailable):
			logger.Info(fmt.Sprintf("table %s:%s: could not read the live column order (%v); using the one from the start of the sync", keyspace, table, err))
		}
	}
	shardReader, _ := (*db).(lib.ShardColumnOrderReader)

	check := columnOrderCheck{shards: make(map[string]*shardColumnOrder, len(shards))}
	for _, shard := range shards {
		so := &shardColumnOrder{}
		check.shards[shard] = so
		saved := streamState.SavedColumnOrder(shard)
		if unsharded {
			// Unsharded keyspace: vtgate's order is the one shard's.
			so.live = slices.Clone(current)
		} else {
			var err error
			if shardReader == nil {
				err = lib.ErrColumnOrderUnavailable
			} else {
				so.live, err = shardReader.ShardColumnOrder(ctx, *psc, keyspace, shard, table)
			}
			if err != nil || len(so.live) == 0 {
				so.live = nil
				logger.Info(fmt.Sprintf("table %s:%s shard %s: could not read the shard's live column order (%v); checking the shard's schema history instead", keyspace, table, shard, err))
			}
		}

		tc, err := streamState.Shards[shard].SerializedCursorToTableCursor()
		if err == nil && tc.Position == "" && tc.LastKnownPk == nil {
			continue
		}
		if len(saved) == 0 {
			continue
		}
		reason := errUnknownShardColumnOrder
		if so.live != nil {
			reason = lib.ColumnOrderChange(saved, so.live)
		}
		if reason == "" {
			continue
		}
		check.changedShards = append(check.changedShards, shard)
		if check.changed == nil {
			check.changed = &ColumnOrderChangedError{
				Keyspace: keyspace,
				Table:    table,
				Reason:   reason,
				Saved:    slices.Clone(saved),
				Current:  slices.Clone(so.live),
				Shard:    shard,
			}
		}
	}
	return check
}

// recordShardColumnOrder saves order as the shard's column order (see
// lib.ShardStates.RecordShardColumnOrder).
func recordShardColumnOrder(state *lib.SyncState, keyspace, table, shard string, order []string) {
	stateKey := keyspace + ":" + table
	streamState := state.Keyspaces[keyspace].Streams[stateKey]
	streamState.RecordShardColumnOrder(shard, order)
	state.Keyspaces[keyspace].Streams[stateKey] = streamState
}

// verifySchemaHistory decides what to do about a column order change: it scans
// every given shard (the shards whose order changed, sorted) with a saved
// binlog position (lib.SchemaHistoryScanner, in shard order) and returns a nil error when each shard either has no schema change on
// the table since its cursor or has one followed by a schema version, so the
// historian decodes the older events correctly (tracked has the shards that
// had one). A shard whose binary logs at the saved position have expired
// gets its cursor reset here, like lib.Read does, so the shard is copied again
// and nothing is decoded from before the change. Otherwise it returns the shard
// that failed and:
//
//   - orderErr with Untracked set for the first shard whose schema change has
//     no schema version after it (Task: enable the tracker, then re-sync);
//   - orderErr unchanged when the client cannot scan, or the scan ran out of
//     time without getting anywhere (Task: re-sync);
//   - a *schemaScanIncompleteError when the scan ran out of time after some
//     progress: that progress is saved on the shard's cursor
//     (SerializedCursor.SchemaScan) and the next sync resumes there;
//   - a *lib.SuspectedBinlogExpirationError when vtgate found no tablet for the
//     saved position (handled like the same error from lib.Read), but not when
//     reading the current position failed (lib.ErrSchemaHistoryPositionCheck);
//   - the scan error, e.g. lib.ErrSchemaHistoryInconclusive (a plain error, so
//     Fivetran retries and the next sync scans again).
//
// tracked has every shard whose verdict was lib.SchemaHistoryTracked, with
// the range the scan verified (see trackedStepOver).
func verifySchemaHistory(ctx context.Context, db *lib.ConnectClient, logger Serializer, psc *lib.PlanetScaleSource, streamState lib.ShardStates, orderErr *ColumnOrderChangedError, order columnOrderCheck) (tracked map[string]trackedRange, failedShard string, err error) {
	shards := order.changedShards
	scanner, ok := (*db).(lib.SchemaHistoryScanner)
	if !ok {
		return nil, "", orderErr
	}
	tabletType := psdbconnect.TabletType_primary
	if psc.UseReplica {
		tabletType = psdbconnect.TabletType_replica
	}
	for _, shard := range shards {
		tc, err := streamState.Shards[shard].SerializedCursorToTableCursor()
		if err != nil {
			return nil, shard, status.Error(codes.Internal, fmt.Sprintf("invalid cursor for stream %v:%v, failed with [%v]", orderErr.Keyspace, orderErr.Table, err))
		}
		if tc.Position == "" {
			continue
		}
		resume := streamState.Shards[shard].SchemaScan
		history, err := scanner.ScanSchemaHistory(ctx, logger, *psc, orderErr.Table, tc, resume)
		if history.Progress != nil {
			// Saved whatever the outcome: the scan read every event up to
			// Progress.Position, and only this cursor's next scan uses it.
			streamState.Shards[shard].SchemaScan = history.Progress
		}
		switch {
		case errors.Is(err, lib.ErrSchemaHistoryPositionCheck):
			// No tablet for the current position (e.g. during a reparent) is
			// not a sign of expired binlogs at the saved one: a plain error, so
			// Fivetran retries and the next sync scans again.
			return nil, shard, errors.Wrapf(err, "checking the schema history of table %s on shard %s after its column order changed (%s)", orderErr.Table, shard, orderErr.Reason)
		case errors.Is(err, lib.ErrSchemaHistoryUnavailable):
			if uErr := unknownShardOrderError(order, orderErr, shard, err); uErr != nil {
				return nil, shard, uErr
			}
			return nil, shard, orderErr
		case errors.Is(err, lib.ErrSchemaHistoryScanTimeout):
			logger.Severe(fmt.Sprintf("table %s:%s shard %s: %v", orderErr.Keyspace, orderErr.Table, shard, err))
			if history.Progress != nil && (resume == nil || history.Progress.Position != resume.Position) {
				return nil, shard, &schemaScanIncompleteError{Table: orderErr.Table, Shard: shard, Err: err}
			}
			if uErr := unknownShardOrderError(order, orderErr, shard, err); uErr != nil {
				return nil, shard, uErr
			}
			// No progress at all: the next sync would not get further.
			return nil, shard, orderErr
		case lib.IsBinlogsExpirationError(err):
			if rErr := resetExpiredCursor(logger, streamState, shard, tc, orderErr.Table, err); rErr != nil {
				return nil, shard, rErr
			}
			continue
		case lib.IsVStreamTabletNotFoundError(err, tabletType):
			return nil, shard, &lib.SuspectedBinlogExpirationError{Keyspace: tc.Keyspace, Shard: shard, Table: orderErr.Table, Err: err}
		case err != nil:
			return nil, shard, errors.Wrapf(err, "checking the schema history of table %s on shard %s after its column order changed (%s)", orderErr.Table, shard, orderErr.Reason)
		}
		if history.Verdict == lib.SchemaHistoryUntracked {
			orderErr.Untracked, orderErr.Shard, orderErr.DDL = true, shard, history.DDL
			return nil, shard, orderErr
		}
		if history.Verdict == lib.SchemaHistoryTracked {
			if tracked == nil {
				tracked = map[string]trackedRange{}
			}
			r := trackedRange{cursor: tc.Position, ddls: history.DDLCount}
			if history.Progress != nil {
				r.verifiedTo = history.Progress.Position
			}
			tracked[shard] = r
		}
		logger.Info(fmt.Sprintf("table %s:%s shard %s: %s", orderErr.Keyspace, orderErr.Table, shard, history.Verdict))
	}
	return tracked, "", nil
}

// trackedRange is the binlog range of one shard that the schema history scan
// of this sync found tracked (lib.SchemaHistoryTracked): from the shard's
// saved cursor to the position the scan read up to ("" if unknown).
type trackedRange struct {
	cursor, verifiedTo string
	// ddls is the number of DDL events on the table in the range, or -1.
	ddls int
}

// stepOverCandidates returns, for every changed shard of the table, the
// selected columns that a tracked step-over may leave out of older binlog
// events (see lib.TrackedStepOver and trackedStepOver). It runs before the
// schema history scan: a later schema change on the table (for example a
// MODIFY that drops the default of the new column) then lands inside the
// scanned range and makes the DDL count more than one, so the column
// definitions read here are those of the one schema change the step-over
// trusts.
//
// Only columns that were plainly added on the shard qualify: the shard's live
// order must keep every column of its saved order (a rename or drop in the
// same change gets the Task: a renamed column has values that the older
// events carry under the old name), the column must be selected, and SHOW
// COLUMNS on the shard must show it nullable, without a default, not
// generated or auto-increment, and not in the primary key
// (lib.ColumnDetail.StepOverBlocker). Online DDL fills a new column for the
// existing rows on the shadow table without row events for the table, so any
// other column would stay NULL in the destination for rows not changed since.
// When in doubt (unknown live order, no saved order, SHOW COLUMNS failing) a
// shard gets nothing.
func stepOverCandidates(ctx context.Context, db *lib.ConnectClient, logger Serializer, psc *lib.PlanetScaleSource, keyspace, table string, streamState lib.ShardStates, order columnOrderCheck, selected []string) map[string][]string {
	var candidates map[string][]string
	for _, shard := range order.changedShards {
		prefix := fmt.Sprintf("table %s:%s shard %s: ", keyspace, table, shard)
		saved := streamState.SavedColumnOrder(shard)
		so := order.shards[shard]
		if len(saved) == 0 || so == nil || so.live == nil {
			continue
		}
		added, onlyAdded := lib.AddedColumns(saved, so.live)
		if !onlyAdded {
			logger.Info(prefix + "the column change also drops or renames a column; a selected new column will not be stepped over")
			continue
		}
		var wanted []string
		for _, c := range added {
			for _, sel := range selected {
				if strings.EqualFold(c, sel) {
					wanted = append(wanted, c)
					break
				}
			}
		}
		if len(wanted) == 0 {
			continue
		}
		reader, ok := (*db).(lib.ShardColumnDetailsReader)
		if !ok {
			continue
		}
		details, err := reader.ShardColumnDetails(ctx, *psc, keyspace, shard, table)
		if err != nil {
			logger.Info(fmt.Sprintf("%scould not read the shard's column details (%v); a selected new column will not be stepped over", prefix, err))
			continue
		}
		var eligible []string
		for _, c := range wanted {
			blocker := "it is not in SHOW COLUMNS on the shard"
			for _, d := range details {
				if strings.EqualFold(d.Name, c) {
					blocker = d.StepOverBlocker()
					break
				}
			}
			if blocker != "" {
				logger.Info(fmt.Sprintf("%snew column %s will not be stepped over: %s", prefix, c, blocker))
				continue
			}
			eligible = append(eligible, c)
		}
		if len(eligible) > 0 {
			if candidates == nil {
				candidates = map[string][]string{}
			}
			candidates[shard] = eligible
		}
	}
	return candidates
}

// trackedStepOver returns what Read may step over on one shard whose schema
// history scan in this sync found every schema change tracked (see
// lib.TrackedStepOver), or nil to keep the plain behaviour (vttablet's error
// raises the re-sync Task). With the tracker on, vttablet resolves the
// VStream filter against the schema each older event was written with, so a
// selected column added after the saved cursor makes it fail on those events.
// It needs the candidates from stepOverCandidates, a known end of the scanned
// range, and exactly one DDL on the table in that range: with more, the
// column definitions read before the scan may not be those of the change that
// added the column (the second change may have dropped a default after the
// backfill), and the stream would need a step-over per cut-over.
func trackedStepOver(logger Serializer, keyspace, table, shard string, r trackedRange, columns []string) *lib.TrackedStepOver {
	if len(columns) == 0 || r.cursor == "" || r.verifiedTo == "" {
		return nil
	}
	prefix := fmt.Sprintf("table %s:%s shard %s: ", keyspace, table, shard)
	if r.ddls != 1 {
		logger.Info(fmt.Sprintf("%sthe schema history check saw %d schema changes on the table (-1: unknown); a selected new column will not be stepped over", prefix, r.ddls))
		return nil
	}
	logger.Info(fmt.Sprintf("%snew column(s) %s may be left out of binlog events up to %s, written before the tracked schema change that added them",
		prefix, strings.Join(columns, ","), r.verifiedTo))
	return &lib.TrackedStepOver{Shard: shard, Cursor: r.cursor, VerifiedTo: r.verifiedTo, Columns: slices.Clone(columns)}
}

// pendingColumnOrder is a shard's live column order waiting to be saved.
//
// The order was read (checkColumnOrder) from some tablet of the shard, and
// bound is the shard's primary position read after it
// (lib.PrimaryPositionReader). Every tablet has applied a subset of the
// primary's GTIDs, so every schema change the order reflects is in bound, and
// once the shard's cursor contains bound the order is not ahead of it. That
// holds with UseReplica too, where the order and the VStream can come from
// different replicas (one past a cut-over, the other not), and during a copy
// that spans several syncs: the copy's checkpoints carry its binlog position,
// so the order is saved with the first checkpoint that covers bound instead
// of only after a read that reached the current position.
//
// A client without lib.PrimaryPositionReader keeps the earlier rule: save
// after a read that reached the current position.
type pendingColumnOrder struct {
	keyspace, table, shard string
	order                  []string
	bound                  string
	afterCompleteRead      bool
}

func newPendingColumnOrder(ctx context.Context, db *lib.ConnectClient, logger Serializer, psc *lib.PlanetScaleSource, state *lib.SyncState, keyspace, table, shard string, so *shardColumnOrder) *pendingColumnOrder {
	p := &pendingColumnOrder{keyspace: keyspace, table: table, shard: shard}
	if so == nil || so.live == nil {
		return p
	}
	if own, ok := state.Keyspaces[keyspace].Streams[keyspace+":"+table].ShardColumnOrders[shard]; ok && slices.Equal(own, so.live) {
		return p
	}
	reader, ok := (*db).(lib.PrimaryPositionReader)
	if !ok {
		p.order, p.afterCompleteRead = so.live, true
		return p
	}
	bound, err := reader.PrimaryPosition(ctx, *psc, keyspace, shard, table)
	if err != nil || bound == "" {
		logger.Info(fmt.Sprintf("table %s:%s shard %s: could not read the primary position (%v); the live column order is not saved in this sync", keyspace, table, shard, err))
		return p
	}
	p.order, p.bound = so.live, bound
	return p
}

// saveIfCovered saves the order once position contains bound.
func (p *pendingColumnOrder) saveIfCovered(state *lib.SyncState, position string) {
	if p.order == nil || p.afterCompleteRead || position == "" {
		return
	}
	if ok, err := lib.PositionAtLeast(position, p.bound); err != nil || !ok {
		return
	}
	recordShardColumnOrder(state, p.keyspace, p.table, p.shard, p.order)
	p.order = nil
}

// saveAfterRead is called after a read that ended without an error, with the
// shard's cursor position.
func (p *pendingColumnOrder) saveAfterRead(state *lib.SyncState, readComplete bool, position string) {
	if p.order != nil && p.afterCompleteRead {
		if readComplete {
			recordShardColumnOrder(state, p.keyspace, p.table, p.shard, p.order)
			p.order = nil
		}
		return
	}
	p.saveIfCovered(state, position)
}

// unknownShardOrderError returns a plain error (Fivetran retries) when the
// shard's live column order could not be read (e.g. a SHOW COLUMNS timeout
// during a reparent) and its schema history could not be checked either:
// there may be no schema change at all, so no Task. It returns nil when the
// shard's live order is known.
func unknownShardOrderError(order columnOrderCheck, orderErr *ColumnOrderChangedError, shard string, err error) error {
	if so := order.shards[shard]; so == nil || so.live != nil {
		return nil
	}
	return errors.Wrapf(err, "checking the schema history of table %s on shard %s, whose live column order could not be read", orderErr.Table, shard)
}

// schemaScanIncompleteError: the schema history scan of a shard ran out of
// time after some progress, saved on the shard's cursor. The table is skipped
// for this sync with a plain error (no Task): the next sync resumes the scan.
type schemaScanIncompleteError struct {
	Table string
	Shard string
	Err   error
}

func (e *schemaScanIncompleteError) Error() string {
	return fmt.Sprintf("the schema history check of table %s on shard %s has not reached the current binlog position yet (%v); "+
		"the next sync resumes it from where it stopped", e.Table, e.Shard, e.Err)
}

func (e *schemaScanIncompleteError) Unwrap() error { return e.Err }

// resetExpiredCursor resets a shard's cursor whose binary logs have expired,
// the same way lib.Read does: the shard is copied again by the next Read and
// the reset goes into the combined binlog expiration Warning.
func resetExpiredCursor(logger Serializer, streamState lib.ShardStates, shard string, tc *psdbconnect.TableCursor, table string, cause error) error {
	logger.Info(fmt.Sprintf("table %s:%s shard %s: the binary logs at the saved position have expired; resetting the cursor to copy the shard again", tc.Keyspace, table, shard))
	if recorder, ok := logger.(lib.BinlogExpirationRecorder); ok {
		recorder.RecordBinlogExpiration(tc.Keyspace, table, shard)
	}
	sc, err := lib.TableCursorToSerializedCursor(&psdbconnect.TableCursor{Keyspace: tc.Keyspace, Shard: tc.Shard})
	if err != nil {
		return errors.Wrap(err, "unable to serialize reset cursor after binlog expiration")
	}
	sc.SetBinlogExpirationError(fmt.Sprintf("Binlogs have expired. Cursor reset to trigger historical sync. Original error: %v", cause.Error()))
	streamState.Shards[shard] = sc
	return nil
}

// isConnectionWideError reports errors that every other table in the
// connection would hit as well, so the sync stops instead of trying each
// remaining table: credentials rejected by vtgate (Unauthenticated,
// PermissionDenied), or vtgate itself unreachable (Unavailable from the gRPC
// transport). An Unavailable error that vtgate relays from a vttablet (one
// shard without a healthy tablet) is not: other keyspaces or shards may still
// sync. DeadlineExceeded is not either: lib.Read handles timeouts per table.
func isConnectionWideError(err error) bool {
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return true
	case codes.Unavailable:
		msg := err.Error()
		return strings.Contains(msg, "connection error") && !strings.Contains(msg, "vttablet")
	default:
		return false
	}
}

// resyncTaskMessage returns the customer-facing Fivetran Task text for read
// errors that cannot be recovered without a historical re-sync, or "" if the
// error should keep the existing retry/error behaviour.
//
// Fivetran de-duplicates Tasks and Warnings by message content, so messages
// must only contain values that are stable between retries (no error text,
// GTIDs or timestamps).
func resyncTaskMessage(err error, keyspace, table, shard string) string {
	var (
		suspected  *lib.SuspectedBinlogExpirationError
		orderError *ColumnOrderChangedError
	)
	switch {
	// The masked vtgate form of purged binlogs, seen on a second consecutive
	// sync (the first one only marks the cursor, see handle). The cursor is
	// kept: "Resync all data" truncates and re-copies. The explicit purged-binlog
	// error never gets here: lib.Read resets that table's cursor (#69/#73).
	case errors.As(err, &suspected):
		return fmt.Sprintf("Re-sync required: the PlanetScale binary logs needed to resume table %q in keyspace %q (shard %q) have expired, "+
			"so incremental replication cannot continue from the saved position. "+
			"To fix this, go to the connection's Settings tab in Fivetran and click \"Resync all data\".",
			table, keyspace, shard)
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
	// A column order change without the schema tracker: the older events can
	// only be decoded correctly once the tracker records schema versions, and
	// those recorded from now on do not cover the events already written, so
	// a re-sync is needed after turning it on.
	case errors.As(err, &orderError) && orderError.Untracked:
		return fmt.Sprintf("Re-sync required: a schema change on table %q in PlanetScale keyspace %q changed its columns "+
			"(a column added, dropped, renamed or moved), and the keyspace does not track "+
			"schema versions, so changes made before the schema change cannot be read with the right column names. "+
			"To fix this, enable the VTTablet parameter track_schema_versions for keyspace %q in PlanetScale (with schema-version-max-age-seconds "+
			"set to 0 or at least as long as the binary log retention), then go to the connection's Settings tab in Fivetran and click \"Resync all data\".",
			table, keyspace, keyspace)
	// Any other column order change (the client could not check the schema
	// history) gets the same text: it is the same kind of schema change, and
	// the text must not name columns.
	case lib.IsVStreamSchemaIncompatibilityError(err), errors.As(err, &orderError):
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
