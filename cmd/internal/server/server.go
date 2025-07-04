package server

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/planetscale/fivetran-source/cmd/internal/server/handlers"

	"github.com/planetscale/fivetran-source/lib"

	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pkg/errors"
	fivetran_sdk_v2 "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
)

type connectorServer struct {
	configurationForm      ConfigurationFormHandler
	schema                 SchemaHandler
	sync                   SyncHandler
	checkConnection        CheckConnectionHandler
	clientConstructor      edgeClientConstructor
	mysqlClientConstructor mysqlClientConstructor
	fivetran_sdk_v2.UnimplementedSourceConnectorServer
}

type (
	edgeClientConstructor  func() lib.ConnectClient
	mysqlClientConstructor func() lib.MysqlClient
)

func NewConnectorServer() fivetran_sdk_v2.SourceConnectorServer {
	return &connectorServer{
		configurationForm: NewConfigurationFormHandler(),
		sync:              NewSyncHandler(),
		schema:            NewSchemaHandler(),
		checkConnection:   NewCheckConnectionHandler(),
	}
}

func (c *connectorServer) ConfigurationForm(ctx context.Context, request *fivetran_sdk_v2.ConfigurationFormRequest) (*fivetran_sdk_v2.ConfigurationFormResponse, error) {
	rLogger := newRequestLogger(newRequestID())

	rLogger.Println("handling configuration form request")
	return c.configurationForm.Handle(ctx, request)
}

func (c *connectorServer) Test(ctx context.Context, request *fivetran_sdk_v2.TestRequest) (*fivetran_sdk_v2.TestResponse, error) {
	rLogger := newRequestLogger(newRequestID())

	rLogger.Println("handling test request")
	if request.Name != handlers.CheckConnectionTestName {
		return nil, status.Errorf(codes.InvalidArgument, "%s is not a valid test", request.Name)
	}

	psc, err := SourceFromRequest(request)
	if err != nil {
		return nil, errors.Wrap(err, "request did not contain a valid configuration")
	}
	mysql, err := lib.NewMySQL(psc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open connection to PlanetScale database")
	}
	defer mysql.Close()
	var db lib.ConnectClient
	if c.clientConstructor == nil {
		db = lib.NewConnectClient(&mysql)
	} else {
		db = c.clientConstructor()
	}
	return c.checkConnection.Handle(ctx, db, request.Name, psc)
}

func (c *connectorServer) Schema(ctx context.Context, request *fivetran_sdk_v2.SchemaRequest) (*fivetran_sdk_v2.SchemaResponse, error) {
	logger := newRequestLogger(newRequestID())

	logger.Println("handling schema request")
	psc, err := SourceFromRequest(request)
	if err != nil {
		return nil, errors.Wrap(err, "request did not contain a valid configuration")
	}

	var (
		mysqlClient lib.MysqlClient
		db          lib.ConnectClient
	)

	if c.mysqlClientConstructor == nil {
		mysqlClient, err = lib.NewMySQL(psc)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "unable to open connection to PlanetScale database")
		}
		defer mysqlClient.Close()
	} else {
		mysqlClient = c.mysqlClientConstructor()
	}

	if mysqlClient == nil {
		panic("mysql client is not initialized")
	}

	if c.clientConstructor == nil {
		db = lib.NewConnectClient(&mysqlClient)
	} else {
		db = c.clientConstructor()
	}

	// check if credentials are still valid.
	checkConn, err := c.checkConnection.Handle(context.Background(), db, handlers.CheckConnectionTestName, psc)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "unable to open connection to PlanetScale database")
	}

	if checkConn.GetFailure() != "" {
		return nil, status.Errorf(codes.InvalidArgument, "unable to connect to PlanetScale database, failed with : %q", checkConn.GetFailure())
	}

	return c.schema.Handle(ctx, psc, &mysqlClient)
}

func (c *connectorServer) Update(request *fivetran_sdk_v2.UpdateRequest, server fivetran_sdk_v2.SourceConnector_UpdateServer) error {
	requestId := newRequestID()
	rLogger := newRequestLogger(requestId)

	rLogger.Println("handling update request")
	psc, err := SourceFromRequest(request)
	if err != nil {
		return status.Error(codes.InvalidArgument, "request did not contain a valid configuration")
	}

	if request.Selection == nil {
		return status.Error(codes.InvalidArgument, "request did not contain a valid selection")
	}

	schemaSelection, ok := request.Selection.Selection.(*fivetran_sdk_v2.Selection_WithSchema)
	if !ok {
		return status.Error(codes.InvalidArgument, "request did not contain a Selection_WithSchema")
	}

	var (
		db          lib.ConnectClient
		mysqlClient lib.MysqlClient
	)

	if c.mysqlClientConstructor == nil {
		mysqlClient, err = lib.NewMySQL(psc)
		if err != nil {
			return status.Error(codes.InvalidArgument, "unable to open connection to PlanetScale database")
		}
	} else {
		mysqlClient = c.mysqlClientConstructor()
	}
	defer mysqlClient.Close()

	if c.clientConstructor == nil {
		db = lib.NewConnectClient(&mysqlClient)
	} else {
		db = c.clientConstructor()
	}

	schemaBuilder := handlers.NewSchemaBuilder(psc.TreatTinyIntAsBoolean)
	if err := mysqlClient.BuildSchema(context.Background(), *psc, schemaBuilder); err != nil {
		status.Error(codes.InvalidArgument, "unable to build schema from PlanetScale database")
		return nil
	}

	sourceSchema, err := schemaBuilder.(*handlers.FiveTranSchemaBuilder).BuildUpdateResponse()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "unable get source schema for this database : %q", err)
	}

	logger := handlers.NewSchemaAwareSerializer(server, requestId, psc.TreatTinyIntAsBoolean, sourceSchema.SchemaList, sourceSchema.EnumsAndSets)

	shards, err := db.ListShards(context.Background(), *psc)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "unable to list shards for this database : %q", err)
	}
	var state *lib.SyncState
	state, err = StateFromRequest(logger, request, *psc, shards, *schemaSelection)
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("request did not contain a valid stateJson : %q", err))
	}

	// check if credentials are still valid.
	checkConn, err := c.checkConnection.Handle(context.Background(), db, handlers.CheckConnectionTestName, psc)
	if err != nil {
		msg := fmt.Sprintf("unable to connect to PlanetScale database, failed with : %q", err)
		logger.Severe(msg)
		return status.Error(codes.NotFound, msg)
	}

	if checkConn.GetFailure() != "" {
		msg := fmt.Sprintf("unable to connect to PlanetScale database, failed with : %q", checkConn.GetFailure())
		logger.Severe(msg)
		return status.Error(codes.NotFound, msg)
	}

	return c.sync.Handle(psc, &db, logger, state, schemaSelection)
}

func newRequestLogger(requestID string) *log.Logger {
	return log.New(os.Stdout, requestID+"  ", log.LstdFlags)
}

func newRequestID() string {
	id := uuid.New()
	return "request-" + id.String()
}
