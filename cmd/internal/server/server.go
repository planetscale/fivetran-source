package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/planetscale/fivetran-source/cmd/internal/server/handlers"

	"github.com/planetscale/fivetran-source/lib"

	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pkg/errors"
	fivetran_sdk "github.com/planetscale/fivetran-sdk-grpc/go"
)

type connectorServer struct {
	configurationForm      ConfigurationFormHandler
	schema                 SchemaHandler
	sync                   SyncHandler
	checkConnection        CheckConnectionHandler
	clientConstructor      edgeClientConstructor
	mysqlClientConstructor mysqlClientConstructor
	fivetran_sdk.UnimplementedConnectorServer
}

type (
	edgeClientConstructor  func() lib.ConnectClient
	mysqlClientConstructor func() lib.MysqlClient
)

func NewConnectorServer() fivetran_sdk.ConnectorServer {
	return &connectorServer{
		configurationForm: NewConfigurationFormHandler(),
		sync:              NewSyncHandler(),
		schema:            NewSchemaHandler(),
		checkConnection:   NewCheckConnectionHandler(),
	}
}

func (c *connectorServer) ConfigurationForm(ctx context.Context, request *fivetran_sdk.ConfigurationFormRequest) (*fivetran_sdk.ConfigurationFormResponse, error) {
	rLogger := newRequestLogger(newRequestID())

	rLogger.Println("handling configuration form request")
	return c.configurationForm.Handle(ctx, request)
}

func (c *connectorServer) Test(ctx context.Context, request *fivetran_sdk.TestRequest) (*fivetran_sdk.TestResponse, error) {
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

func (c *connectorServer) Schema(ctx context.Context, request *fivetran_sdk.SchemaRequest) (*fivetran_sdk.SchemaResponse, error) {
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

func (c *connectorServer) Update(request *fivetran_sdk.UpdateRequest, server fivetran_sdk.Connector_UpdateServer) error {
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

	schemaSelection, ok := request.Selection.Selection.(*fivetran_sdk.Selection_WithSchema)
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sourceSchema, err := c.schema.Handle(ctx, psc, &mysqlClient)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "unable get source schema for this database : %q", err)
	}

	logger := handlers.NewSchemaAwareSerializer(server, requestId, psc.TreatTinyIntAsBoolean, sourceSchema.GetWithSchema())

	shards, err := db.ListShards(ctx, *psc)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "unable to list shards for this database : %q", err)
	}
	var state *lib.SyncState
	state, err = StateFromRequest(request, *psc, shards, *schemaSelection)
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("request did not contain a valid stateJson : %q", err))
	}

	// check if credentials are still valid.
	checkConn, err := c.checkConnection.Handle(context.Background(), db, handlers.CheckConnectionTestName, psc)
	if err != nil {
		logger.Log(fivetran_sdk.LogLevel_SEVERE, fmt.Sprintf("unable to connect to PlanetScale database, failed with : %q", err))
		return nil
	}

	if checkConn.GetFailure() != "" {
		logger.Log(fivetran_sdk.LogLevel_SEVERE, fmt.Sprintf("unable to connect to PlanetScale database, failed with : %q", checkConn.GetFailure()))
		return nil
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
