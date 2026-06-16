package lib

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/auth"
	grpcclient "github.com/planetscale/psdb/core/pool"
	clientoptions "github.com/planetscale/psdb/core/pool/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"

	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

type (
	OnResult func(*sqltypes.Result, Operation) error
	OnUpdate func(*UpdatedRow) error
	OnCursor func(*psdbconnect.TableCursor) error
)

type DatabaseLogger interface {
	Info(string) error
	Warning(string) error
}

var (
	cursorCheckpointRows       = 1000
	cursorCheckpointInterval   = 10 * time.Minute
	maxConsecutiveSyncTimeouts = 5
)

// ConnectClient is a general purpose interface
// that defines all the data access methods needed for the PlanetScale Fivetran source to function.
type ConnectClient interface {
	CanConnect(ctx context.Context, ps PlanetScaleSource) error
	Read(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, columns []string, lastKnownPosition *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor, onUpdate OnUpdate) (*SerializedCursor, error)
	ListShards(ctx context.Context, ps PlanetScaleSource) ([]string, error)
}

func NewConnectClient(mysqlAccess *MysqlClient) ConnectClient {
	return &connectClient{
		Mysql: mysqlAccess,
	}
}

// connectClient is an implementation of the ConnectClient interface defined above.
// It uses the mysql interface provided by PlanetScale for all schema/shard/tablet discovery and
// the grpc API for incrementally syncing rows from PlanetScale.
type connectClient struct {
	clientFn        func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error)
	vstreamClientFn func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error)
	Mysql           *MysqlClient
}

type vstreamClient interface {
	VStream(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error)
}

func (p connectClient) ListShards(ctx context.Context, ps PlanetScaleSource) ([]string, error) {
	return (*p.Mysql).GetVitessShards(ctx, ps)
}

func (p connectClient) CanConnect(ctx context.Context, ps PlanetScaleSource) error {
	if *p.Mysql == nil {
		return status.Error(codes.Internal, "Mysql access is uninitialized")
	}

	if err := p.checkEdgePassword(ctx, ps); err != nil {
		return errors.Wrap(err, "Unable to initialize Connect Session")
	}

	return (*p.Mysql).PingContext(ctx, ps)
}

func (p connectClient) checkEdgePassword(ctx context.Context, psc PlanetScaleSource) error {
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, fmt.Sprintf("https://%v", psc.Host), nil)
	if err != nil {
		return err
	}

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		return errors.Errorf("The database %q, hosted at %q, is inaccessible from this process", psc.Database, psc.Host)
	}

	return nil
}

// Read streams rows from a table given a starting cursor.
// 1. We will get the latest vgtid for a given table in a shard when a sync session starts.
// 2. This latest vgtid is now the stopping point for this sync session.
// 3. Ask vstream to stream from the last known vgtid
// 4. When we reach the stopping point, read all rows available at this vgtid
// 5. End the stream when (a) a vgtid newer than latest vgtid is encountered or (b) the timeout kicks in.
func (p connectClient) Read(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, columns []string, lastKnownPosition *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor, onUpdate OnUpdate) (*SerializedCursor, error) {
	var (
		err                     error
		sErr                    error
		currentSerializedCursor *SerializedCursor
	)

	tabletType := psdbconnect.TabletType_primary
	if ps.UseReplica {
		tabletType = psdbconnect.TabletType_replica
	}

	currentPosition := lastKnownPosition
	readDuration := 1 * time.Minute
	preamble := fmt.Sprintf("[%v:%v shard:%v tabletType:%s] ", ps.Database, tableName, currentPosition.Shard, tabletType)

	// Timeout tracking variables
	consecutiveTimeouts := 0
	const maxTimeout = 1 * time.Hour
	const timeoutMultiplier = 2.8
	backoffDuration := 10 * time.Second

	existingColumns, err := p.filterExistingColumns(ctx, ps, tableName, columns)
	if err != nil {
		logger.Info(fmt.Sprintf("%sCouldn't fetch existing columns, falling back to requested columns: %s", preamble, err.Error()))
	}

	logger.Info(fmt.Sprintf("%sFiltering with columns %s", preamble, strings.Join(existingColumns, ",")))
	logger.Info(fmt.Sprintf("%sUsing read timeout: %v", preamble, readDuration))

	for {
		logger.Info(preamble + "peeking to see if there's any new rows")
		latestCursorPosition, lcErr := p.getLatestCursorPosition(ctx, currentPosition.Shard, currentPosition.Keyspace, tableName, ps, tabletType)
		if lcErr != nil {
			return currentSerializedCursor, errors.Wrap(lcErr, "Unable to get latest cursor position")
		}

		// The current vgtid is the same as the last synced vgtid, no new rows.
		// A LastKnownPk cursor is still in the historical copy phase and must
		// resume even when the binlog position has not advanced.
		if currentPosition.LastKnownPk == nil && latestCursorPosition == currentPosition.Position {
			logger.Info(preamble + "no new rows found, exiting")
			return TableCursorToSerializedCursor(currentPosition)
		}
		logger.Info(fmt.Sprintf(preamble+"new rows found, syncing rows for %v", readDuration))
		logger.Info(fmt.Sprintf(preamble+"syncing rows with cursor [%v]", currentPosition))

		previousPosition := cloneTableCursor(currentPosition)
		currentPosition, err = p.sync(ctx, logger, tableName, existingColumns, currentPosition, latestCursorPosition, ps, tabletType, readDuration, onResult, onCursor, onUpdate)
		madeProgress := tableCursorMadeProgress(previousPosition, currentPosition)
		if tableCursorHasProgress(currentPosition) {
			currentSerializedCursor, sErr = TableCursorToSerializedCursor(currentPosition)
			if sErr != nil {
				// if we failed to serialize here, we should bail.
				return currentSerializedCursor, errors.Wrap(sErr, "unable to serialize current position")
			}
		}
		if err != nil {
			if s, ok := status.FromError(err); ok {
				// if the error is anything other than server timeout, keep going
				if s.Code() != codes.DeadlineExceeded {
					logger.Warning(fmt.Sprintf("%vGot error [%v] with message [%q], returning error with cursor :[%v] after non-timeout error", preamble, s.Code(), err, currentPosition))

					// Check for binlog expiration error and reset cursor for historical sync
					if IsBinlogsExpirationError(err) {
						logger.Warning(fmt.Sprintf("%sBinlogs have expired. Resetting cursor position to trigger historical sync", preamble))
						// Reset the cursor position to empty to trigger historical sync on next iteration
						currentPosition.Position = ""
						currentPosition.LastKnownPk = nil

						currentSerializedCursor, sErr = TableCursorToSerializedCursor(currentPosition)
						if sErr != nil {
							return currentSerializedCursor, errors.Wrap(sErr, "unable to serialize reset cursor after binlog expiration")
						}
						currentSerializedCursor.SetBinlogExpirationError(fmt.Sprintf("Binlogs have expired. Cursor reset to trigger historical sync. Original error: %v", err.Error()))

						// Continue with historical sync instead of returning error
						continue
					}

					if IsVStreamSchemaIncompatibilityError(err) {
						return currentSerializedCursor, errors.Wrapf(err, "PlanetScale VStream cannot continue incremental replication for table %s after a schema change; run a Fivetran historical re-sync for this connection to recover", tableName)
					}

					return currentSerializedCursor, err
				}

				newReadDuration := time.Duration(float64(readDuration) * timeoutMultiplier)
				readDuration = func() time.Duration {
					if newReadDuration > maxTimeout {
						return maxTimeout
					}
					return newReadDuration
				}()

				if madeProgress {
					if consecutiveTimeouts > 0 {
						logger.Info(fmt.Sprintf("%sTimeout occurred after cursor progress; resetting no-progress timeout counter (was %d)", preamble, consecutiveTimeouts))
					} else {
						logger.Info(fmt.Sprintf("%sTimeout occurred after cursor progress; continuing without incrementing no-progress timeout counter", preamble))
					}
					consecutiveTimeouts = 0
					backoffDuration = 10 * time.Second
					logger.Info(fmt.Sprintf("%sIncreased read timeout to: %v", preamble, readDuration))
					continue
				}

				consecutiveTimeouts++
				logger.Info(fmt.Sprintf("%sTimeout occurred without cursor progress (%d/%d consecutive no-progress timeouts)", preamble, consecutiveTimeouts, maxConsecutiveSyncTimeouts))

				if consecutiveTimeouts >= maxConsecutiveSyncTimeouts {
					logger.Info(fmt.Sprintf("%sReached maximum consecutive no-progress timeouts (%d), stopping sync", preamble, maxConsecutiveSyncTimeouts))

					warningMessage := fmt.Sprintf("Timeout occurred while reading table %s after %d consecutive attempts without cursor progress. Stopping sync.", tableName, consecutiveTimeouts)

					if serializer, ok := logger.(interface {
						SendWarningAlert(string) error
					}); ok {
						if err := serializer.SendWarningAlert(warningMessage); err != nil {
							logger.Warning(fmt.Sprintf("Failed to send warning message: %v", err))
						}
					} else {
						// Fallback to regular warning log if serializer doesn't support SendWarningAlert
						logger.Warning(warningMessage)
					}

					return currentSerializedCursor, nil
				}

				// Apply exponential backoff only for timeouts that did not move the cursor.
				logger.Info(fmt.Sprintf("%sApplying backoff delay: %v", preamble, backoffDuration))
				backoffTimer := time.NewTimer(backoffDuration)
				select {
				case <-ctx.Done():
					if !backoffTimer.Stop() {
						<-backoffTimer.C
					}
					return currentSerializedCursor, ctx.Err()
				case <-backoffTimer.C:
				}
				backoffDuration = time.Duration(math.Min(float64(backoffDuration)*2, float64(5*time.Minute))) // Cap at 5 minutes

				logger.Info(fmt.Sprintf("%sIncreased read timeout to: %v", preamble, readDuration))
				logger.Info(fmt.Sprintf("%sContinuing with cursor after server timeout without progress (attempt %d/%d)", preamble, consecutiveTimeouts, maxConsecutiveSyncTimeouts))
			} else if errors.Is(err, io.EOF) {
				logger.Info(fmt.Sprintf("%vFinished reading all rows for table [%v]", preamble, tableName))
				return currentSerializedCursor, nil
			} else {
				logger.Warning(fmt.Sprintf(preamble+"non-grpc error [%v]]", err))
				return currentSerializedCursor, err
			}
		}
	}
}

func (p connectClient) sync(ctx context.Context, logger DatabaseLogger, tableName string, columns []string, tc *psdbconnect.TableCursor, stopPosition string, ps PlanetScaleSource, tabletType psdbconnect.TabletType, readDuration time.Duration, onResult OnResult, onCursor OnCursor, onUpdate OnUpdate) (*psdbconnect.TableCursor, error) {
	ctx, cancel := context.WithTimeout(ctx, readDuration)
	defer cancel()

	var (
		err    error
		client vstreamClient
		close  func()
	)

	preamble := fmt.Sprintf("[%v:%v shard:%v tabletType:%s] ", ps.Database, tableName, tc.Shard, tabletType)

	client, close, err = p.newVStreamClient(ctx, ps)
	if err != nil {
		return tc, err
	}
	defer close()

	syncStartCursor := cloneTableCursor(tc)

	logger.Info(fmt.Sprintf("%sSyncing with cursor position : [%v], using last known PK : %v, stop cursor is : [%v]", preamble, tc.Position, tc.LastKnownPk != nil, stopPosition))

	c, err := client.VStream(ctx, buildVStreamRequest(tableName, columns, tc, tabletType))
	if err != nil {
		return tc, err
	}

	// stop when we've reached the well known stop position for this sync session.
	watchForVgGtidChange := false
	recordsSinceCheckpoint := 0
	lastCheckpoint := time.Now()
	fieldsByTable := map[string][]*query.Field{}
	for {
		res, err := c.Recv()
		if err != nil {
			return tc, err
		}

		copyCompleted := false
		for _, event := range res.Events {
			var (
				eventRecords       int
				eventCopyCompleted bool
			)
			tc, eventRecords, eventCopyCompleted, err = handleVStreamEvent(tableName, tc, event, fieldsByTable, onResult, onUpdate)
			if err != nil {
				return syncStartCursor, err
			}
			recordsSinceCheckpoint += eventRecords
			copyCompleted = copyCompleted || eventCopyCompleted
		}

		// A single VGTID can appear in multiple ordered responses. Once we reach
		// the desired stop VGTID, keep reading while the cursor stays there so all
		// rows for that position are processed, then stop when a newer VGTID arrives.
		watchForVgGtidChange = watchForVgGtidChange || tc.Position == stopPosition

		if shouldCheckpointCursor(tc, recordsSinceCheckpoint, lastCheckpoint) && onCursor != nil {
			if err := onCursor(tc); err != nil {
				return tc, status.Error(codes.Internal, "unable to serialize cursor")
			}
			recordsSinceCheckpoint = 0
			lastCheckpoint = time.Now()
		}

		if copyCompleted || (watchForVgGtidChange && tc.LastKnownPk == nil && tc.Position != stopPosition) {
			if onCursor != nil {
				if err := onCursor(tc); err != nil {
					return tc, status.Error(codes.Internal, "unable to serialize cursor")
				}
			}
			return tc, io.EOF
		}
	}
}

func (p connectClient) newVStreamClient(ctx context.Context, ps PlanetScaleSource) (vstreamClient, func(), error) {
	if p.vstreamClientFn != nil {
		client, err := p.vstreamClientFn(ctx, ps)
		return client, func() {}, err
	}
	if p.clientFn != nil {
		client, err := p.clientFn(ctx, ps)
		if err != nil {
			return nil, func() {}, err
		}
		return connectSyncCompatVStreamClient{client: client}, func() {}, nil
	}

	conn, err := grpcclient.Dial(
		ctx, ps.Host,
		clientoptions.WithDefaultTLSConfig(),
		clientoptions.WithCompression(true),
		clientoptions.WithConnectionPool(1),
		clientoptions.WithExtraCallOption(
			auth.NewBasicAuth(ps.Username, ps.Password).CallOption(),
		),
	)
	if err != nil {
		return nil, func() {}, err
	}
	return vtgateservicepb.NewVitessClient(conn), func() { _ = conn.Close() }, nil
}

type connectSyncCompatVStreamClient struct {
	client psdbconnect.ConnectClient
}

func (c connectSyncCompatVStreamClient) VStream(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
	syncReq := syncRequestFromVStreamRequest(in)
	stream, err := c.client.Sync(ctx, syncReq, opts...)
	if err != nil {
		return nil, err
	}
	return &connectSyncCompatVStream{
		tableName: syncReq.TableName,
		stream:    stream,
	}, nil
}

type connectSyncCompatVStream struct {
	tableName string
	stream    psdbconnect.Connect_SyncClient
	grpc.ClientStream
}

func (s *connectSyncCompatVStream) Recv() (*vtgatepb.VStreamResponse, error) {
	res, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}
	return vstreamResponseFromSyncResponse(s.tableName, res), nil
}

func syncRequestFromVStreamRequest(req *vtgatepb.VStreamRequest) *psdbconnect.SyncRequest {
	tableName := tableNameFromVStreamRequest(req)
	return &psdbconnect.SyncRequest{
		TableName:      tableName,
		Cursor:         tableCursorFromVStreamRequest(req, tableName),
		TabletType:     fromVStreamTabletType(req.GetTabletType()),
		IncludeUpdates: true,
		IncludeInserts: true,
		IncludeDeletes: true,
		Columns:        columnsFromVStreamRequest(req),
		Cells:          cellsFromVStreamRequest(req),
	}
}

func tableNameFromVStreamRequest(req *vtgatepb.VStreamRequest) string {
	for _, rule := range req.GetFilter().GetRules() {
		if rule.GetMatch() != "" {
			return rule.GetMatch()
		}
	}
	return ""
}

func tableCursorFromVStreamRequest(req *vtgatepb.VStreamRequest, tableName string) *psdbconnect.TableCursor {
	cursor := &psdbconnect.TableCursor{}
	shardGtids := req.GetVgtid().GetShardGtids()
	if len(shardGtids) == 0 {
		return cursor
	}
	shardGtid := shardGtids[0]
	cursor.Shard = shardGtid.Shard
	cursor.Keyspace = shardGtid.Keyspace
	cursor.Position = shardGtid.Gtid
	cursor.LastKnownPk = lastKnownPKForTable(shardGtid.TablePKs, tableName)
	return cursor
}

func columnsFromVStreamRequest(req *vtgatepb.VStreamRequest) []string {
	for _, rule := range req.GetFilter().GetRules() {
		columns := columnsFromVStreamFilter(rule.GetFilter())
		if columns != nil {
			return columns
		}
	}
	return nil
}

func columnsFromVStreamFilter(filter string) []string {
	upper := strings.ToUpper(filter)
	fromIdx := strings.LastIndex(upper, " FROM ")
	if !strings.HasPrefix(upper, "SELECT ") || fromIdx < len("SELECT ") {
		return nil
	}
	columnList := strings.TrimSpace(filter[len("SELECT "):fromIdx])
	if columnList == "" || columnList == "*" {
		return nil
	}
	parts := strings.Split(columnList, ",")
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		column := unquoteVStreamIdentifier(strings.TrimSpace(part))
		if column != "" {
			columns = append(columns, column)
		}
	}
	if len(columns) == 0 {
		return nil
	}
	return columns
}

func unquoteVStreamIdentifier(identifier string) string {
	if len(identifier) >= 2 && identifier[0] == '`' && identifier[len(identifier)-1] == '`' {
		return strings.ReplaceAll(identifier[1:len(identifier)-1], "``", "`")
	}
	return identifier
}

func cellsFromVStreamRequest(req *vtgatepb.VStreamRequest) []string {
	cells := req.GetFlags().GetCells()
	if cells == "" {
		return nil
	}
	return strings.Split(cells, ",")
}

func vstreamResponseFromSyncResponse(tableName string, res *psdbconnect.SyncResponse) *vtgatepb.VStreamResponse {
	events := []*binlogdatapb.VEvent{}
	for _, result := range res.GetResult() {
		events = append(events, vstreamRowEventsFromQueryResult(tableName, result, func(row *query.Row) *binlogdatapb.RowChange {
			return &binlogdatapb.RowChange{After: row}
		})...)
	}
	for _, deleted := range res.GetDeletes() {
		events = append(events, vstreamRowEventsFromQueryResult(tableName, deleted.GetResult(), func(row *query.Row) *binlogdatapb.RowChange {
			return &binlogdatapb.RowChange{Before: row}
		})...)
	}
	for _, updated := range res.GetUpdates() {
		events = append(events, vstreamRowEventsFromUpdate(tableName, updated)...)
	}
	if res.GetCursor() != nil {
		events = append(events, vstreamVGtidEventFromCursor(tableName, res.GetCursor()))
	}
	return &vtgatepb.VStreamResponse{Events: events}
}

func vstreamRowEventsFromQueryResult(tableName string, result *query.QueryResult, rowChange func(*query.Row) *binlogdatapb.RowChange) []*binlogdatapb.VEvent {
	if result == nil {
		return nil
	}
	events := []*binlogdatapb.VEvent{}
	if len(result.Fields) > 0 {
		events = append(events, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_FIELD,
			FieldEvent: &binlogdatapb.FieldEvent{
				TableName: tableName,
				Fields:    result.Fields,
			},
		})
	}
	changes := make([]*binlogdatapb.RowChange, 0, len(result.Rows))
	for _, row := range result.Rows {
		changes = append(changes, rowChange(row))
	}
	if len(changes) > 0 {
		events = append(events, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_ROW,
			RowEvent: &binlogdatapb.RowEvent{
				TableName:  tableName,
				RowChanges: changes,
			},
		})
	}
	return events
}

func vstreamRowEventsFromUpdate(tableName string, updated *psdbconnect.UpdatedRow) []*binlogdatapb.VEvent {
	if updated == nil || (updated.Before == nil && updated.After == nil) {
		return nil
	}
	fields := updated.GetAfter().GetFields()
	if len(fields) == 0 {
		fields = updated.GetBefore().GetFields()
	}
	events := []*binlogdatapb.VEvent{}
	if len(fields) > 0 {
		events = append(events, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_FIELD,
			FieldEvent: &binlogdatapb.FieldEvent{
				TableName: tableName,
				Fields:    fields,
			},
		})
	}

	beforeRows := updated.GetBefore().GetRows()
	afterRows := updated.GetAfter().GetRows()
	rowCount := len(beforeRows)
	if len(afterRows) > rowCount {
		rowCount = len(afterRows)
	}
	changes := make([]*binlogdatapb.RowChange, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		change := &binlogdatapb.RowChange{}
		if i < len(beforeRows) {
			change.Before = beforeRows[i]
		}
		if i < len(afterRows) {
			change.After = afterRows[i]
		}
		changes = append(changes, change)
	}
	if len(changes) > 0 {
		events = append(events, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_ROW,
			RowEvent: &binlogdatapb.RowEvent{
				TableName:  tableName,
				RowChanges: changes,
			},
		})
	}
	return events
}

func vstreamVGtidEventFromCursor(tableName string, cursor *psdbconnect.TableCursor) *binlogdatapb.VEvent {
	shardGtid := &binlogdatapb.ShardGtid{
		Keyspace: cursor.Keyspace,
		Shard:    cursor.Shard,
		Gtid:     cursor.Position,
	}
	if cursor.LastKnownPk != nil {
		shardGtid.TablePKs = []*binlogdatapb.TableLastPK{{
			TableName: tableName,
			Lastpk:    cursor.LastKnownPk,
		}}
	}
	return &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_VGTID,
		Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{shardGtid},
		},
	}
}

func buildVStreamRequest(tableName string, columns []string, tc *psdbconnect.TableCursor, tabletType psdbconnect.TabletType) *vtgatepb.VStreamRequest {
	shardGtid := &binlogdatapb.ShardGtid{
		Keyspace: tc.Keyspace,
		Shard:    tc.Shard,
		Gtid:     tc.Position,
	}
	if tc.LastKnownPk != nil {
		shardGtid.TablePKs = []*binlogdatapb.TableLastPK{{
			TableName: tableName,
			Lastpk:    tc.LastKnownPk,
		}}
	}

	return &vtgatepb.VStreamRequest{
		TabletType: toVStreamTabletType(tabletType),
		Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{shardGtid},
		},
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  tableName,
				Filter: "SELECT " + joinVStreamColumns(columns) + " FROM " + quoteVStreamIdentifier(tableName),
			}},
		},
		Flags: &vtgatepb.VStreamFlags{
			MinimizeSkew: true,
			Cells:        "planetscale_operator_default",
		},
	}
}

func toVStreamTabletType(tabletType psdbconnect.TabletType) topodatapb.TabletType {
	switch tabletType {
	case psdbconnect.TabletType_replica:
		return topodatapb.TabletType_REPLICA
	case psdbconnect.TabletType_batch:
		return topodatapb.TabletType_RDONLY
	default:
		return topodatapb.TabletType_PRIMARY
	}
}

func fromVStreamTabletType(tabletType topodatapb.TabletType) psdbconnect.TabletType {
	switch tabletType {
	case topodatapb.TabletType_REPLICA:
		return psdbconnect.TabletType_replica
	case topodatapb.TabletType_RDONLY:
		return psdbconnect.TabletType_batch
	default:
		return psdbconnect.TabletType_primary
	}
}

func joinVStreamColumns(columns []string) string {
	if len(columns) == 0 {
		return "*"
	}
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		column = strings.TrimSpace(column)
		if column == "" {
			continue
		}
		quoted = append(quoted, quoteVStreamIdentifier(column))
	}
	if len(quoted) == 0 {
		return "*"
	}
	return strings.Join(quoted, ",")
}

func quoteVStreamIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func handleVStreamEvent(tableName string, cursor *psdbconnect.TableCursor, event *binlogdatapb.VEvent, fieldsByTable map[string][]*query.Field, onResult OnResult, onUpdate OnUpdate) (*psdbconnect.TableCursor, int, bool, error) {
	switch event.Type {
	case binlogdatapb.VEventType_FIELD:
		if event.FieldEvent != nil {
			cacheVStreamFields(fieldsByTable, event.FieldEvent.TableName, event.FieldEvent.Fields)
		}
	case binlogdatapb.VEventType_ROW:
		count, err := handleVStreamRows(tableName, event.RowEvent, fieldsByTable, onResult, onUpdate)
		if err != nil {
			return cursor, 0, false, err
		}
		return cursor, count, false, nil
	case binlogdatapb.VEventType_VGTID:
		next, err := tableCursorFromVGtid(cursor, event.Vgtid, tableName)
		return next, 0, false, err
	case binlogdatapb.VEventType_LASTPK:
		next, err := tableCursorFromLastPK(cursor, event.LastPKEvent, tableName)
		return next, 0, false, err
	case binlogdatapb.VEventType_COPY_COMPLETED:
		return cursor, 0, true, nil
	}
	return cursor, 0, false, nil
}

func handleVStreamRows(tableName string, rowEvent *binlogdatapb.RowEvent, fieldsByTable map[string][]*query.Field, onResult OnResult, onUpdate OnUpdate) (int, error) {
	if rowEvent == nil || normalizeVStreamTableName(rowEvent.TableName) != tableName {
		return 0, nil
	}
	fields := vstreamFieldsForTable(fieldsByTable, rowEvent.TableName)
	if len(fields) == 0 {
		return 0, status.Error(codes.Internal, fmt.Sprintf("missing VStream fields for table %s", rowEvent.TableName))
	}

	records := 0
	for _, change := range rowEvent.RowChanges {
		switch {
		case change.After != nil && change.Before == nil:
			if onResult != nil {
				if err := onResult(vstreamRowResult(fields, change.After), OpType_Insert); err != nil {
					return records, status.Error(codes.Internal, "unable to serialize row")
				}
			}
			records++
		case change.After == nil && change.Before != nil:
			if onResult != nil {
				if err := onResult(vstreamRowResult(fields, change.Before), OpType_Delete); err != nil {
					return records, status.Error(codes.Internal, "unable to serialize row")
				}
			}
			records++
		case change.After != nil && change.Before != nil:
			if onUpdate != nil {
				if err := onUpdate(&UpdatedRow{
					Before: vstreamRowResult(fields, change.Before),
					After:  vstreamRowResult(fields, change.After),
				}); err != nil {
					return records, status.Error(codes.Internal, "unable to serialize update")
				}
			}
			records++
		}
	}
	return records, nil
}

func vstreamRowResult(fields []*query.Field, row *query.Row) *sqltypes.Result {
	return sqltypes.Proto3ToResult(&query.QueryResult{
		Fields: fields,
		Rows:   []*query.Row{row},
	})
}

func cacheVStreamFields(fieldsByTable map[string][]*query.Field, tableName string, fields []*query.Field) {
	fieldsByTable[tableName] = fields
	fieldsByTable[normalizeVStreamTableName(tableName)] = fields
}

func vstreamFieldsForTable(fieldsByTable map[string][]*query.Field, tableName string) []*query.Field {
	if fields := fieldsByTable[tableName]; len(fields) > 0 {
		return fields
	}
	return fieldsByTable[normalizeVStreamTableName(tableName)]
}

func normalizeVStreamTableName(tableName string) string {
	if idx := strings.LastIndex(tableName, "."); idx >= 0 {
		return tableName[idx+1:]
	}
	return tableName
}

func tableCursorFromVGtid(cursor *psdbconnect.TableCursor, vgtid *binlogdatapb.VGtid, tableName string) (*psdbconnect.TableCursor, error) {
	if vgtid == nil {
		return cursor, nil
	}
	for _, shardGtid := range vgtid.ShardGtids {
		if shardGtid.Keyspace != cursor.Keyspace || shardGtid.Shard != cursor.Shard {
			continue
		}
		next := cloneTableCursor(cursor)
		if shardGtid.Gtid != "" {
			next.Position = shardGtid.Gtid
		}
		next.LastKnownPk = lastKnownPKForTable(shardGtid.TablePKs, tableName)
		if err := validateLastKnownPKFields(next.LastKnownPk, tableName); err != nil {
			return cursor, err
		}
		return next, nil
	}
	return cursor, nil
}

func tableCursorFromLastPK(cursor *psdbconnect.TableCursor, event *binlogdatapb.LastPKEvent, tableName string) (*psdbconnect.TableCursor, error) {
	if event == nil || event.TableLastPK == nil || normalizeVStreamTableName(event.TableLastPK.TableName) != tableName {
		return cursor, nil
	}
	next := cloneTableCursor(cursor)
	if event.Completed {
		next.LastKnownPk = nil
		return next, nil
	}
	next.LastKnownPk = event.TableLastPK.Lastpk
	if err := validateLastKnownPKFields(next.LastKnownPk, tableName); err != nil {
		return cursor, err
	}
	return next, nil
}

func lastKnownPKForTable(tablePKs []*binlogdatapb.TableLastPK, tableName string) *query.QueryResult {
	for _, tablePK := range tablePKs {
		if tablePK != nil && normalizeVStreamTableName(tablePK.TableName) == tableName {
			return tablePK.Lastpk
		}
	}
	return nil
}

func validateLastKnownPKFields(lastPK *query.QueryResult, tableName string) error {
	if lastPK == nil || len(lastPK.Fields) > 0 {
		return nil
	}
	return status.Error(codes.Internal, fmt.Sprintf("VStream copy cursor for table %s is missing LastKnownPk field metadata", tableName))
}

func tableCursorHasProgress(tc *psdbconnect.TableCursor) bool {
	return tc != nil && (tc.Position != "" || tc.LastKnownPk != nil)
}

func tableCursorMadeProgress(before, after *psdbconnect.TableCursor) bool {
	if after == nil {
		return false
	}
	if before == nil {
		return tableCursorHasProgress(after)
	}
	return before.Position != after.Position || !proto.Equal(before.LastKnownPk, after.LastKnownPk)
}

func shouldCheckpointCursor(tc *psdbconnect.TableCursor, recordsSinceCheckpoint int, lastCheckpoint time.Time) bool {
	if tc == nil || tc.LastKnownPk == nil || recordsSinceCheckpoint == 0 {
		return false
	}
	if cursorCheckpointRows > 0 && recordsSinceCheckpoint >= cursorCheckpointRows {
		return true
	}
	if cursorCheckpointInterval > 0 && time.Since(lastCheckpoint) >= cursorCheckpointInterval {
		return true
	}
	return false
}

func cloneTableCursor(tc *psdbconnect.TableCursor) *psdbconnect.TableCursor {
	if tc == nil {
		return nil
	}
	return proto.Clone(tc).(*psdbconnect.TableCursor)
}

func (p connectClient) filterExistingColumns(ctx context.Context, ps PlanetScaleSource, tableName string, columns []string) ([]string, error) {
	existingColumns := []string{}
	results, err := (*p.Mysql).GetKeyspaceTableColumns(ctx, ps.Database, tableName)
	if err != nil {
		existingColumns = columns
	} else {
		columnSet := map[string]bool{}
		for _, result := range results {
			columnSet[result.Name] = true
		}

		for _, c := range columns {
			if columnSet[c] {
				existingColumns = append(existingColumns, c)
			}
		}

	}
	return existingColumns, err
}

func (p connectClient) getLatestCursorPosition(ctx context.Context, shard, keyspace string, tableName string, ps PlanetScaleSource, tabletType psdbconnect.TabletType) (string, error) {
	timeout := 45 * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var (
		err    error
		client vstreamClient
		close  func()
	)

	client, close, err = p.newVStreamClient(ctx, ps)
	if err != nil {
		return "", err
	}
	defer close()

	c, err := client.VStream(ctx, buildVStreamRequest(tableName, nil, &psdbconnect.TableCursor{
		Shard:    shard,
		Keyspace: keyspace,
		Position: "current",
	}, tabletType))
	if err != nil {
		return "", err
	}

	for {
		res, err := c.Recv()
		if err != nil {
			return "", err
		}

		position := vgtidPositionForShard(res.GetEvents(), keyspace, shard)
		if position != "" {
			return position, nil
		}
	}
}

func vgtidPositionForShard(events []*binlogdatapb.VEvent, keyspace, shard string) string {
	for _, event := range events {
		if event.GetType() != binlogdatapb.VEventType_VGTID {
			continue
		}
		for _, shardGtid := range event.GetVgtid().GetShardGtids() {
			if shardGtid.Keyspace == keyspace && shardGtid.Shard == shard && shardGtid.Gtid != "" {
				return shardGtid.Gtid
			}
		}
	}
	return ""
}

func IsBinlogsExpirationError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Cannot replicate because the source purged required binary logs")
}

func IsVStreamSchemaIncompatibilityError(err error) bool {
	if err == nil {
		return false
	}

	message := err.Error()
	if !strings.Contains(message, "Code: FAILED_PRECONDITION") {
		return false
	}
	if !strings.Contains(message, "failed to build table replication plan") {
		return false
	}

	return strings.Contains(message, "cannot use column names in vstream filter") ||
		(strings.Contains(message, "column ") && strings.Contains(message, " not found in table "))
}
