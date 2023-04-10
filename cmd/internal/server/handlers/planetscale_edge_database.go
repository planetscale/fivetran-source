package handlers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-proto/go"
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
	Read(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, table *fivetransdk.TableSelection, tc *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor) (*SerializedCursor, error)
	DiscoverSchema(ctx context.Context, ps PlanetScaleSource) (*fivetransdk.SchemaResponse, error)
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
	if p.Mysql == nil {
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

// DiscoverSchema returns schemas for all tables in a PlanetScale database
// 1. Get all keyspaces for the PlanetScale database
// 2. Get the schemas for all tables in a keyspace, for each keyspace
// 2. Get columns and primary keys for each table from information_schema.columns
// 3. Format results into FiveTran response
func (p PlanetScaleEdgeDatabase) DiscoverSchema(ctx context.Context, ps PlanetScaleSource) (*fivetransdk.SchemaResponse, error) {
	keyspaces, err := (*p.Mysql).GetKeyspaces(ctx, ps)
	if err != nil {
		return nil, errors.Errorf("unable to query database %q, hosted at %q, for keyspaces: %+v", ps.Database, ps.Host, err)
	}

	schemas := make([]*fivetransdk.Schema, len(keyspaces))

	for i, keyspace := range keyspaces {
		schema := &fivetransdk.Schema{
			Name:   ps.Database,
			Tables: []*fivetransdk.Table{},
		}

		tables, err := (*p.Mysql).GetKeyspaceTableNames(ctx, keyspace)
		if err != nil {
			return nil, errors.Errorf("unable to query keyspace %s of database %q, hosted at %q, for schema: %+v", keyspace, ps.Database, ps.Host, err)
		}

		for _, tableName := range tables {
			table, err := p.getSchemaForTable(ctx, ps, tableName, keyspace)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to get stream for table %v", tableName)
			}
			schema.Tables = append(schema.Tables, table)
		}

		schemas[i] = schema
	}

	resp := fivetransdk.SchemaResponse{
		Response: &fivetransdk.SchemaResponse_WithSchema{
			WithSchema: &fivetransdk.SchemaList{
				Schemas: schemas,
			},
		},
	}

	return &resp, nil
}

// geSchemaForTable gets the table schema for a table in a specific keyspace of a PlanetScale database
func (p PlanetScaleEdgeDatabase) getSchemaForTable(ctx context.Context, psc PlanetScaleSource, tableName string, keyspaceName string) (*fivetransdk.Table, error) {
	schema := fivetransdk.Table{
		Name: tableName,
	}

	var err error
	columns, err := (*p.Mysql).GetKeyspaceTableSchema(ctx, psc, tableName, keyspaceName)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get column names & types for table %v", tableName)
	}

	primaryKeys, err := (*p.Mysql).GetKeyspaceTablePrimaryKeys(ctx, tableName, keyspaceName)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to iterate primary keys for table %s", tableName)
	}

	for _, key := range primaryKeys {
		c := columns[key]
		c.IsPrimaryKey = true
	}

	for columnName, column := range columns {
		schema.Columns = append(schema.Columns, &fivetransdk.Column{
			Name:       columnName,
			Type:       column.Type,
			PrimaryKey: column.IsPrimaryKey,
		})
	}

	return &schema, nil
}

// Read streams rows from a table given a starting cursor.
// 1. We will get the latest vgtid for a given table in a shard when a sync session starts.
// 2. This latest vgtid is now the stopping point for this sync session.
// 3. Ask vstream to stream from the last known vgtid
// 4. When we reach the stopping point, read all rows available at this vgtid
// 5. End the stream when (a) a vgtid newer than latest vgtid is encountered or (b) the timeout kicks in.
func (p PlanetScaleEdgeDatabase) Read(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, table *fivetransdk.TableSelection, lastKnownPosition *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor) (*SerializedCursor, error) {
	var (
		err                     error
		sErr                    error
		currentSerializedCursor *SerializedCursor
	)

	tabletType := psdbconnect.TabletType_primary
	currentPosition := lastKnownPosition
	readDuration := 1 * time.Minute
	preamble := fmt.Sprintf("[%v:%v shard : %v] ", ps.Database, table.TableName, currentPosition.Shard)
	for {
		logger.Log(fivetransdk.LogLevel_INFO, preamble+"peeking to see if there's any new rows")
		latestCursorPosition, lcErr := p.getLatestCursorPosition(ctx, currentPosition.Shard, currentPosition.Keyspace, table.TableName, ps, tabletType)
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

		currentPosition, err = p.sync(ctx, logger, table.TableName, currentPosition, latestCursorPosition, ps, tabletType, readDuration, onResult, onCursor)
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
				logger.Log(fivetransdk.LogLevel_INFO, fmt.Sprintf("%vFinished reading all rows for table [%v]", preamble, table.TableName))
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

// Convert columnType to fivetran type
func getFivetranDataType(mType string, treatTinyIntAsBoolean bool) fivetransdk.DataType {
	mysqlType := strings.ToLower(mType)
	if strings.HasPrefix(mysqlType, "tinyint") {
		if treatTinyIntAsBoolean {
			return fivetransdk.DataType_BOOLEAN
		}

		return fivetransdk.DataType_INT
	}

	// A BIT(n) type can have a length of 1 to 64
	// we serialize these as a LONG : int64 value when serializing rows.
	if strings.HasPrefix(mysqlType, "bit") {
		return fivetransdk.DataType_LONG
	}

	if strings.HasPrefix(mysqlType, "varbinary") {
		return fivetransdk.DataType_BINARY
	}

	if strings.HasPrefix(mysqlType, "int") {
		if strings.Contains(mysqlType, "unsigned") {
			return fivetransdk.DataType_LONG
		}
		return fivetransdk.DataType_INT
	}

	if strings.HasPrefix(mysqlType, "smallint") {
		return fivetransdk.DataType_INT
	}

	if strings.HasPrefix(mysqlType, "bigint") {
		return fivetransdk.DataType_LONG
	}

	if strings.HasPrefix(mysqlType, "decimal") {
		return fivetransdk.DataType_DECIMAL
	}

	if strings.HasPrefix(mysqlType, "double") ||
		strings.HasPrefix(mysqlType, "float") {
		return fivetransdk.DataType_DOUBLE
	}

	if strings.HasPrefix(mysqlType, "timestamp") {
		return fivetransdk.DataType_UTC_DATETIME
	}

	if strings.HasPrefix(mysqlType, "time") {
		return fivetransdk.DataType_STRING
	}

	if strings.HasPrefix(mysqlType, "datetime") {
		return fivetransdk.DataType_NAIVE_DATETIME
	}

	if strings.HasPrefix(mysqlType, "year") {
		return fivetransdk.DataType_INT
	}

	if strings.HasPrefix(mysqlType, "varchar") ||
		strings.HasPrefix(mysqlType, "text") ||
		strings.HasPrefix(mysqlType, "enum") ||
		strings.HasPrefix(mysqlType, "char") {
		return fivetransdk.DataType_STRING
	}

	if strings.HasPrefix(mysqlType, "set") ||
		strings.HasPrefix(mysqlType, "geometry") ||
		strings.HasPrefix(mysqlType, "geometrycollection") ||
		strings.HasPrefix(mysqlType, "multipoint") ||
		strings.HasPrefix(mysqlType, "multipolygon") ||
		strings.HasPrefix(mysqlType, "polygon") ||
		strings.HasPrefix(mysqlType, "point") ||
		strings.HasPrefix(mysqlType, "linestring") ||
		strings.HasPrefix(mysqlType, "multilinestring") {
		return fivetransdk.DataType_JSON
	}

	switch mysqlType {
	case "date":
		return fivetransdk.DataType_NAIVE_DATE
	case "json":
		return fivetransdk.DataType_JSON
	case "tinytext":
		return fivetransdk.DataType_STRING
	case "mediumtext":
		return fivetransdk.DataType_STRING
	case "longtext":
		return fivetransdk.DataType_STRING
	case "binary":
		return fivetransdk.DataType_BINARY
	case "blob":
		return fivetransdk.DataType_BINARY
	case "longblob":
		return fivetransdk.DataType_BINARY
	case "mediumblob":
		return fivetransdk.DataType_BINARY
	case "tinyblob":
		return fivetransdk.DataType_BINARY
	case "bit":
		return fivetransdk.DataType_BOOLEAN
	case "time":
		return fivetransdk.DataType_STRING
	default:
		return fivetransdk.DataType_STRING
	}
}
