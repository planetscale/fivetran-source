package lib

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/proto/query"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/auth"
	grpcclient "github.com/planetscale/psdb/core/pool"
	clientoptions "github.com/planetscale/psdb/core/pool/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
}

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
	clientFn func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error)
	Mysql    *MysqlClient
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
	const maxConsecutiveTimeouts = 5
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
			return currentSerializedCursor, errors.Wrap(err, "Unable to get latest cursor position")
		}

		// the current vgtid is the same as the last synced vgtid, no new rows.
		if latestCursorPosition == currentPosition.Position {
			logger.Info(preamble + "no new rows found, exiting")
			return TableCursorToSerializedCursor(currentPosition)
		}
		logger.Info(fmt.Sprintf(preamble+"new rows found, syncing rows for %v", readDuration))
		logger.Info(fmt.Sprintf(preamble+"syncing rows with cursor [%v]", currentPosition))

		currentPosition, err = p.sync(ctx, logger, tableName, existingColumns, currentPosition, latestCursorPosition, ps, tabletType, readDuration, onResult, onCursor, onUpdate)
		if currentPosition.Position != "" {
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
					logger.Info(fmt.Sprintf("%vGot error [%v] with message [%q], Returning with cursor :[%v] after non-timeout error", preamble, s.Code(), err, currentPosition))
					return currentSerializedCursor, nil
				} else {
					consecutiveTimeouts++
					logger.Info(fmt.Sprintf("%sTimeout occurred (%d/%d consecutive timeouts)", preamble, consecutiveTimeouts, maxConsecutiveTimeouts))

					if consecutiveTimeouts >= maxConsecutiveTimeouts {
						logger.Info(fmt.Sprintf("%sReached maximum consecutive timeouts (%d), stopping sync", preamble, maxConsecutiveTimeouts))
						return currentSerializedCursor, nil
					}

					// Apply exponential backoff
					logger.Info(fmt.Sprintf("%sApplying backoff delay: %v", preamble, backoffDuration))
					time.Sleep(backoffDuration)
					backoffDuration = time.Duration(math.Min(float64(backoffDuration)*2, float64(5*time.Minute))) // Cap at 5 minutes

					// Increase readDuration by 20% on every retry
					readDuration = time.Duration(float64(readDuration) * 1.2)
					logger.Info(fmt.Sprintf("%sIncreased read timeout to: %v", preamble, readDuration))

					logger.Info(fmt.Sprintf("%sContinuing with cursor after server timeout (attempt %d/%d)", preamble, consecutiveTimeouts, maxConsecutiveTimeouts))
				}
			} else if errors.Is(err, io.EOF) {
				logger.Info(fmt.Sprintf("%vFinished reading all rows for table [%v]", preamble, tableName))
				return currentSerializedCursor, nil
			} else {
				logger.Info(fmt.Sprintf(preamble+"non-grpc error [%v]]", err))
				return currentSerializedCursor, err
			}
		} else {
			// Reset timeout counter on successful sync
			if consecutiveTimeouts > 0 {
				logger.Info(fmt.Sprintf("%sSync successful, resetting timeout counter (was %d)", preamble, consecutiveTimeouts))
				consecutiveTimeouts = 0
				backoffDuration = 10 * time.Second // Reset backoff
				readDuration = 1 * time.Minute     // Reset read duration to original value
				logger.Info(fmt.Sprintf("%sReset read timeout to: %v", preamble, readDuration))
			}
		}
	}
}

func (p connectClient) sync(ctx context.Context, logger DatabaseLogger, tableName string, columns []string, tc *psdbconnect.TableCursor, stopPosition string, ps PlanetScaleSource, tabletType psdbconnect.TabletType, readDuration time.Duration, onResult OnResult, onCursor OnCursor, onUpdate OnUpdate) (*psdbconnect.TableCursor, error) {
	ctx, cancel := context.WithTimeout(ctx, readDuration)
	defer cancel()

	var (
		err    error
		client psdbconnect.ConnectClient
	)

	preamble := fmt.Sprintf("[%v:%v shard:%v tabletType:%s] ", ps.Database, tableName, tc.Shard, tabletType)

	if p.clientFn == nil {
		conn, err := grpcclient.Dial(ctx, ps.Host,
			clientoptions.WithDefaultTLSConfig(),
			clientoptions.WithCompression(true),
			clientoptions.WithConnectionPool(1),
			clientoptions.WithExtraCallOption(
				auth.NewBasicAuth(ps.Username, ps.Password).CallOption(),
			),
		)
		if err != nil {
			return tc, err
		}
		defer conn.Close()
		client = psdbconnect.NewConnectClient(conn)
	} else {
		client, err = p.clientFn(ctx, ps)
		if err != nil {
			return tc, err
		}
	}

	if tc.LastKnownPk != nil {
		tc.Position = ""
	}

	logger.Info(fmt.Sprintf("%sSyncing with cursor position : [%v], using last known PK : %v, stop cursor is : [%v]", preamble, tc.Position, tc.LastKnownPk != nil, stopPosition))

	sReq := &psdbconnect.SyncRequest{
		TableName:      tableName,
		Cursor:         tc,
		TabletType:     tabletType,
		Columns:        columns,
		IncludeUpdates: true,
		IncludeInserts: true,
		IncludeDeletes: true,
		Cells:          []string{"planetscale_operator_default"},
	}

	c, err := client.Sync(ctx, sReq)
	if err != nil {
		return tc, err
	}

	// stop when we've reached the well known stop position for this sync session.
	watchForVgGtidChange := false
	for {

		res, err := c.Recv()
		if err != nil {
			return tc, err
		}

		if res.Cursor != nil {
			tc = res.Cursor
		}

		// Because of the ordering of events in a vstream
		// we receive the vgtid event first and then the rows.
		// the vgtid event might repeat, but they're ordered.
		// so we once we reach the desired stop vgtid, we stop the sync session
		// if we get a newer vgtid.
		watchForVgGtidChange = watchForVgGtidChange || tc.Position == stopPosition

		if onResult != nil {
			for _, insertedRow := range res.Result {
				qr := sqltypes.Proto3ToResult(insertedRow)
				for _, row := range qr.Rows {
					sqlResult := &sqltypes.Result{
						Fields: insertedRow.Fields,
					}
					sqlResult.Rows = append(sqlResult.Rows, row)
					if err := onResult(sqlResult, OpType_Insert); err != nil {
						return tc, status.Error(codes.Internal, "unable to serialize row")
					}
				}
			}

			for _, deletedRow := range res.Deletes {
				qr := sqltypes.Proto3ToResult(deletedRow.Result)
				for _, row := range qr.Rows {
					sqlResult := &sqltypes.Result{
						Fields: deletedRow.Result.Fields,
					}
					sqlResult.Rows = append(sqlResult.Rows, row)
					if err := onResult(sqlResult, OpType_Delete); err != nil {
						return nil, status.Error(codes.Internal, "unable to serialize row")
					}
				}
			}
		}

		if onUpdate != nil {
			for _, update := range res.Updates {
				updatedRow := &UpdatedRow{
					Before: serializeQueryResult(update.Before),
					After:  serializeQueryResult(update.After),
				}
				if err := onUpdate(updatedRow); err != nil {
					return nil, status.Error(codes.Internal, "unable to serialize update")
				}
			}
		}

		if watchForVgGtidChange && tc.Position != stopPosition {
			if err := onCursor(tc); err != nil {
				return tc, status.Error(codes.Internal, "unable to serialize cursor")
			}
			return tc, io.EOF
		}
	}
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

func serializeQueryResult(result *query.QueryResult) *sqltypes.Result {
	qr := sqltypes.Proto3ToResult(result)
	var sqlResult *sqltypes.Result
	for _, row := range qr.Rows {
		sqlResult = &sqltypes.Result{
			Fields: result.Fields,
		}
		sqlResult.Rows = append(sqlResult.Rows, row)
	}
	return sqlResult
}

func (p connectClient) getLatestCursorPosition(ctx context.Context, shard, keyspace string, tableName string, ps PlanetScaleSource, tabletType psdbconnect.TabletType) (string, error) {
	timeout := 45 * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var (
		err    error
		client psdbconnect.ConnectClient
	)

	if p.clientFn == nil {
		conn, err := grpcclient.Dial(ctx, ps.Host,
			clientoptions.WithDefaultTLSConfig(),
			clientoptions.WithCompression(true),
			clientoptions.WithConnectionPool(1),
			clientoptions.WithExtraCallOption(
				auth.NewBasicAuth(ps.Username, ps.Password).CallOption(),
			),
		)
		if err != nil {
			return "", err
		}
		defer conn.Close()
		client = psdbconnect.NewConnectClient(conn)
	} else {
		client, err = p.clientFn(ctx, ps)
		if err != nil {
			return "", err
		}
	}

	sReq := &psdbconnect.SyncRequest{
		TableName: tableName,
		Cursor: &psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: keyspace,
			Position: "current",
		},
		TabletType: tabletType,
		Cells:      []string{"planetscale_operator_default"},
	}

	c, err := client.Sync(ctx, sReq)
	if err != nil {
		return "", nil
	}

	for {
		res, err := c.Recv()
		if err != nil {
			return "", err
		}

		if res.Cursor != nil {
			return res.Cursor.Position, nil
		}
	}
}
