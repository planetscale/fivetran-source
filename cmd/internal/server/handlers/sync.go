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

func (s *Sync) handle(ctx context.Context, psc *lib.PlanetScaleSource, db *lib.ConnectClient, logger Serializer, state *lib.SyncState, schema *fivetransdk.Selection_WithSchema) error {
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
	)
keyspaces:
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

			// A column added in the middle of the table, or columns reordered,
			// since the saved cursors: without the vttablet schema historian the
			// older binlog events would be decoded by position into the wrong
			// columns. Stream the table only if the schema tracker recorded the
			// change (or the cursors are already past it); otherwise raise the
			// re-sync Task instead of streaming the table.
			order := checkColumnOrder(logger, state, ks.SchemaName, table.TableName, hasShards && !allShardsHaveNoProgress)
			if orderErr := order.changed; orderErr != nil {
				tracked, failedShard, hErr := verifySchemaHistory(ctx, db, logger, psc, streamState, orderErr)
				if hErr == nil {
					why := "the saved cursors are already past the schema change"
					if tracked {
						why = "the schema tracker recorded the schema change, so the older binlog events are decoded with the schema they were written with"
					}
					logger.Info(fmt.Sprintf("table %s: column order changed (%s); %s, syncing as usual", stateKey, orderErr.Reason, why))
					order.addedColumnsSafe = true
				} else {
					var suspected *lib.SuspectedBinlogExpirationError
					if errors.As(hErr, &suspected) && !streamState.Shards[failedShard].HasSuspectedBinlogExpiration() {
						// Same as a Read that fails this way (see below): mark the
						// cursor and fail this sync; the next one raises the Task.
						streamState.Shards[failedShard].SetSuspectedBinlogExpiration()
						if sErr := logger.State(*state); sErr != nil {
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
			// Read may only leave out a column that vttablet reports as "not
			// found" when the saved order is known to be a prefix of the live
			// order (or the schema history check passed), see lib.WithAddedColumnRetry.
			readCtx := ctx
			if order.addedColumnsSafe {
				readCtx = lib.WithAddedColumnRetry(ctx)
			}
			readComplete := true

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
				sc, err := (*db).Read(readCtx, logger, *psc, table.TableName, columns, tc, onRow, onCursor, onUpdate)
				if errors.Is(err, lib.ErrReadStoppedEarly) {
					// Read gave up after repeated timeouts and already sent a Warning:
					// keep the cursor it reached, but the table was not read up to
					// the current position.
					readComplete = false
					err = nil
				}
				if err != nil {
					readComplete = false
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
					if sErr := logger.State(*state); sErr != nil {
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
					logger.State(*state) // Checkpoint after every shard
				} else if shardState.HasSuspectedBinlogExpiration() {
					// The shard synced fine, so the earlier failure was transient.
					shardState.ClearSuspectedBinlogExpiration()
					logger.State(*state)
				}
			}
			// Save the live column order only once every shard was read up to
			// the current position, i.e. past the schema change that produced
			// it. Saved earlier, a cursor still before the change would be
			// compared with an order that does not describe its events.
			if readComplete && order.current != nil {
				recordColumnOrder(state, ks.SchemaName, table.TableName, order.current)
			}
		}
	}
	if sErr := logger.State(*state); sErr != nil {
		return status.Error(codes.Internal, sErr.Error())
	}
	if pendingTask != "" {
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
	// changed is set when the live column order is not the saved order with
	// columns appended (or only dropped), see lib.ColumnOrderChange.
	changed *ColumnOrderChangedError
	// current is the live column order, saved in state once the table was
	// read up to the current position; nil when the Serializer does not know
	// the source schema of the table.
	current []string
	// addedColumnsSafe: the saved order is a prefix of the live order (or the
	// table is copied from scratch), so a column vttablet reports as "not
	// found" at the saved position was added at the end of the table and Read
	// may step over it (lib.WithAddedColumnRetry). False when the saved order
	// is unknown (state from an older connector version): vttablet reports the
	// last live column as "not found" after a column added in the middle of the
	// table too, so Read keeps raising the re-sync Task as before.
	addedColumnsSafe bool
}

// checkColumnOrder compares the column order saved in the table's state with
// the live order from the source schema, if the Serializer knows it (see
// SourceColumnOrderer). It never changes the saved order: the caller saves the
// live order once the table's read completed, and keeps the saved order when
// it raises the Task, so every sync raises it until the connection is
// re-synced (or verifySchemaHistory clears it).
//
// compare is false when no shard of the table has durable progress (initial
// sync, "Resync all data", or a cursor reset after binlog expiration): the
// table is copied from scratch, so only the order is recorded.
func checkColumnOrder(logger Serializer, state *lib.SyncState, keyspace, table string, compare bool) columnOrderCheck {
	orderer, ok := logger.(SourceColumnOrderer)
	if !ok {
		return columnOrderCheck{}
	}
	current, ok := orderer.SourceColumnOrder(keyspace, table)
	if !ok {
		return columnOrderCheck{}
	}
	check := columnOrderCheck{current: slices.Clone(current)}
	if !compare {
		check.addedColumnsSafe = true
		return check
	}
	saved := state.Keyspaces[keyspace].Streams[keyspace+":"+table].ColumnOrder
	if len(saved) == 0 {
		return check
	}
	if reason := lib.ColumnOrderChange(saved, current); reason != "" {
		check.changed = &ColumnOrderChangedError{
			Keyspace: keyspace,
			Table:    table,
			Reason:   reason,
			Saved:    slices.Clone(saved),
			Current:  slices.Clone(current),
		}
		return check
	}
	check.addedColumnsSafe = true
	return check
}

func recordColumnOrder(state *lib.SyncState, keyspace, table string, current []string) {
	stateKey := keyspace + ":" + table
	streamState := state.Keyspaces[keyspace].Streams[stateKey]
	if !slices.Equal(streamState.ColumnOrder, current) {
		streamState.ColumnOrder = slices.Clone(current)
		state.Keyspaces[keyspace].Streams[stateKey] = streamState
	}
}

// verifySchemaHistory decides what to do about a column order change: it scans
// every shard with a saved binlog position (lib.SchemaHistoryScanner, in shard
// order) and returns a nil error when each shard either has no schema change on
// the table since its cursor or has one followed by a schema version, so the
// historian decodes the older events correctly (tracked reports whether any
// shard had one). A shard whose binary logs at the saved position have expired
// gets its cursor reset here, like lib.Read does, so the shard is copied again
// and nothing is decoded from before the change. Otherwise it returns the shard
// that failed and:
//
//   - orderErr with Untracked set for the first shard whose schema change has
//     no schema version after it (Task: enable the tracker, then re-sync);
//   - orderErr unchanged when the client cannot scan, or the scan did not
//     reach the current position in time (Task: re-sync);
//   - a *lib.SuspectedBinlogExpirationError when vtgate found no tablet for the
//     saved position (handled like the same error from lib.Read);
//   - the scan error, e.g. lib.ErrSchemaHistoryInconclusive (a plain error, so
//     Fivetran retries and the next sync scans again).
func verifySchemaHistory(ctx context.Context, db *lib.ConnectClient, logger Serializer, psc *lib.PlanetScaleSource, streamState lib.ShardStates, orderErr *ColumnOrderChangedError) (tracked bool, failedShard string, err error) {
	scanner, ok := (*db).(lib.SchemaHistoryScanner)
	if !ok {
		return false, "", orderErr
	}
	tabletType := psdbconnect.TabletType_primary
	if psc.UseReplica {
		tabletType = psdbconnect.TabletType_replica
	}
	shards := make([]string, 0, len(streamState.Shards))
	for shard := range streamState.Shards {
		shards = append(shards, shard)
	}
	slices.Sort(shards)
	for _, shard := range shards {
		tc, err := streamState.Shards[shard].SerializedCursorToTableCursor()
		if err != nil {
			return false, shard, status.Error(codes.Internal, fmt.Sprintf("invalid cursor for stream %v:%v, failed with [%v]", orderErr.Keyspace, orderErr.Table, err))
		}
		if tc.Position == "" {
			continue
		}
		history, err := scanner.ScanSchemaHistory(ctx, logger, *psc, orderErr.Table, tc)
		switch {
		case errors.Is(err, lib.ErrSchemaHistoryUnavailable):
			return false, shard, orderErr
		case errors.Is(err, lib.ErrSchemaHistoryScanTimeout):
			logger.Severe(fmt.Sprintf("table %s:%s shard %s: %v", orderErr.Keyspace, orderErr.Table, shard, err))
			return false, shard, orderErr
		case lib.IsBinlogsExpirationError(err):
			if rErr := resetExpiredCursor(logger, streamState, shard, tc, orderErr.Table, err); rErr != nil {
				return false, shard, rErr
			}
			continue
		case lib.IsVStreamTabletNotFoundError(err, tabletType):
			return false, shard, &lib.SuspectedBinlogExpirationError{Keyspace: tc.Keyspace, Shard: shard, Table: orderErr.Table, Err: err}
		case err != nil:
			return false, shard, errors.Wrapf(err, "checking the schema history of table %s on shard %s after its column order changed (%s)", orderErr.Table, shard, orderErr.Reason)
		}
		if history.Verdict == lib.SchemaHistoryUntracked {
			orderErr.Untracked, orderErr.Shard, orderErr.DDL = true, shard, history.DDL
			return false, shard, orderErr
		}
		tracked = tracked || history.Verdict == lib.SchemaHistoryTracked
		logger.Info(fmt.Sprintf("table %s:%s shard %s: %s", orderErr.Keyspace, orderErr.Table, shard, history.Verdict))
	}
	return tracked, "", nil
}

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
		return fmt.Sprintf("Re-sync required: a schema change on table %q in PlanetScale keyspace %q changed the order of its columns "+
			"(a column added in the middle of the table, columns reordered, or columns dropped and added), and the keyspace does not track "+
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
