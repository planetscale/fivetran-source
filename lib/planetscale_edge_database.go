package lib

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-proto/proto/fivetransdk/v1alpha1"
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
	OnResult func(*sqltypes.Result) error
	OnCursor func(*psdbconnect.TableCursor) error
)

type DatabaseLogger interface {
	Log(fivetransdk.LogLevel, string) error
}

// PlanetScaleDatabase is a general purpose interface
// that defines all the data access methods needed for the PlanetScale Fivetran source to function.
type PlanetScaleDatabase interface {
	CanConnect(ctx context.Context, ps PlanetScaleSource) error
	Read(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, lastKnownPosition *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor) (*SerializedCursor, error)
	ListShards(ctx context.Context, ps PlanetScaleSource) ([]string, error)
}

func NewEdgeDatabase(mysqlAccess *PlanetScaleEdgeMysqlAccess) PlanetScaleDatabase {
	return &PlanetScaleEdgeDatabase{
		Mysql: mysqlAccess,
	}
}

// PlanetScaleEdgeDatabase is an implementation of the PlanetScaleDatabase interface defined above.
// It uses the mysql interface provided by PlanetScale for all schema/shard/tablet discovery and
// the grpc API for incrementally syncing rows from PlanetScale.
type PlanetScaleEdgeDatabase struct {
	clientFn func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error)
	Mysql    *PlanetScaleEdgeMysqlAccess
}

func (p PlanetScaleEdgeDatabase) ListShards(ctx context.Context, ps PlanetScaleSource) ([]string, error) {
	return (*p.Mysql).GetVitessShards(ctx, ps)
}

func (p PlanetScaleEdgeDatabase) CanConnect(ctx context.Context, ps PlanetScaleSource) error {
	if *p.Mysql == nil {
		return status.Error(codes.Internal, "Mysql access is uninitialized")
	}

	if err := p.checkEdgePassword(ctx, ps); err != nil {
		return errors.Wrap(err, "Unable to initialize Connect Session")
	}

	return (*p.Mysql).PingContext(ctx, ps)
}

func (p PlanetScaleEdgeDatabase) checkEdgePassword(ctx context.Context, psc PlanetScaleSource) error {
	if !strings.HasSuffix(psc.Host, ".connect.psdb.cloud") {
		return errors.New("This password is not connect-enabled, please ensure that your organization is enrolled in the Connect beta.")
	}
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
func (p PlanetScaleEdgeDatabase) Read(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, lastKnownPosition *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor) (*SerializedCursor, error) {
	var (
		err                     error
		sErr                    error
		currentSerializedCursor *SerializedCursor
	)

	tabletType := psdbconnect.TabletType_primary
	currentPosition := lastKnownPosition
	readDuration := 1 * time.Minute
	preamble := fmt.Sprintf("[%v:%v shard : %v] ", ps.Database, tableName, currentPosition.Shard)
	for {
		logger.Log(fivetransdk.LogLevel_INFO, preamble+"peeking to see if there's any new rows")
		latestCursorPosition, lcErr := p.getLatestCursorPosition(ctx, currentPosition.Shard, currentPosition.Keyspace, tableName, ps, tabletType)
		if lcErr != nil {
			return currentSerializedCursor, errors.Wrap(err, "Unable to get latest cursor position")
		}

		// the current vgtid is the same as the last synced vgtid, no new rows.
		if latestCursorPosition == currentPosition.Position {
			logger.Log(fivetransdk.LogLevel_INFO, preamble+"no new rows found, exiting")
			return TableCursorToSerializedCursor(currentPosition)
		}
		logger.Log(fivetransdk.LogLevel_INFO, fmt.Sprintf("new rows found, syncing rows for %v", readDuration))
		logger.Log(fivetransdk.LogLevel_INFO, fmt.Sprintf(preamble+"syncing rows with cursor [%v]", currentPosition))

		currentPosition, err = p.sync(ctx, logger, tableName, currentPosition, latestCursorPosition, ps, tabletType, readDuration, onResult, onCursor)
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
					logger.Log(fivetransdk.LogLevel_INFO, fmt.Sprintf("%v Got error [%v] with message [%q], Returning with cursor :[%v] after server timeout", preamble, s.Code(), err, currentPosition))
					return currentSerializedCursor, nil
				} else {
					logger.Log(fivetransdk.LogLevel_INFO, preamble+"Continuing with cursor after server timeout")
				}
			} else if errors.Is(err, io.EOF) {
				logger.Log(fivetransdk.LogLevel_INFO, fmt.Sprintf("%vFinished reading all rows for table [%v]", preamble, tableName))
				return currentSerializedCursor, nil
			} else {
				logger.Log(fivetransdk.LogLevel_INFO, fmt.Sprintf("non-grpc error [%v]]", err))
				return currentSerializedCursor, err
			}
		}
	}
}

func (p PlanetScaleEdgeDatabase) sync(ctx context.Context, logger DatabaseLogger, tableName string, tc *psdbconnect.TableCursor, stopPosition string, ps PlanetScaleSource, tabletType psdbconnect.TabletType, readDuration time.Duration, onResult OnResult, onCursor OnCursor) (*psdbconnect.TableCursor, error) {
	ctx, cancel := context.WithTimeout(ctx, readDuration)
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

	logger.Log(fivetransdk.LogLevel_INFO, fmt.Sprintf("Syncing with cursor position : [%v], using last known PK : %v, stop cursor is : [%v]", tc.Position, tc.LastKnownPk != nil, stopPosition))

	sReq := &psdbconnect.SyncRequest{
		TableName:  tableName,
		Cursor:     tc,
		TabletType: tabletType,
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

		if len(res.Result) > 0 {
			for _, result := range res.Result {
				qr := sqltypes.Proto3ToResult(result)
				for _, row := range qr.Rows {
					sqlResult := &sqltypes.Result{
						Fields: result.Fields,
					}
					sqlResult.Rows = append(sqlResult.Rows, row)
					if err := onResult(sqlResult); err != nil {
						return tc, status.Error(codes.Internal, "unable to serialize row")
					}
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

func (p PlanetScaleEdgeDatabase) getLatestCursorPosition(ctx context.Context, shard, keyspace string, tableName string, ps PlanetScaleSource, tabletType psdbconnect.TabletType) (string, error) {
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
