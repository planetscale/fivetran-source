package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pkg/errors"
	fivetran_sdk "github.com/planetscale/fivetran-proto/proto/fivetransdk/v1alpha1"
	"github.com/planetscale/fivetran-source/cmd/internal/server/handlers"
)

type connectorServer struct {
	configurationForm ConfigurationFormHandler
	schema            SchemaHandler
	sync              SyncHandler
	checkConnection   CheckConnectionHandler
	clientConstructor edgeClientConstructor
}

type edgeClientConstructor func() handlers.PlanetScaleDatabase

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

	psc, err := handlers.SourceFromRequest(request)
	if err != nil {
		return nil, errors.Wrap(err, "request did not contain a valid configuration")
	}
	mysql, err := handlers.NewMySQL(psc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open connection to PlanetScale database")
	}
	defer mysql.Close()
	var db handlers.PlanetScaleDatabase
	if c.clientConstructor == nil {
		db = handlers.NewEdgeDatabase(&mysql)
	} else {
		db = c.clientConstructor()
	}
	return c.checkConnection.Handle(ctx, db, request.Name, psc)
}

func (c *connectorServer) Schema(ctx context.Context, request *fivetran_sdk.SchemaRequest) (*fivetran_sdk.SchemaResponse, error) {
	logger := newRequestLogger(newRequestID())

	logger.Println("handling schema request")
	psc, err := handlers.SourceFromRequest(request)
	if err != nil {
		return nil, errors.Wrap(err, "request did not contain a valid configuration")
	}

	var db handlers.PlanetScaleDatabase
	if c.clientConstructor == nil {
		mysql, err := handlers.NewMySQL(psc)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "unable to open connection to PlanetScale database")
		}
		defer mysql.Close()
		db = handlers.NewEdgeDatabase(&mysql)
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

	return c.schema.Handle(ctx, psc, &db)
}

func (c *connectorServer) Update(request *fivetran_sdk.UpdateRequest, server fivetran_sdk.Connector_UpdateServer) error {
	requestId := newRequestID()
	rLogger := newRequestLogger(requestId)

	rLogger.Println("handling update request")
	psc, err := handlers.SourceFromRequest(request)
	if err != nil {
		return status.Error(codes.InvalidArgument, "request did not contain a valid configuration")
	}

	if request.Selection == nil {
		return status.Error(codes.InvalidArgument, "request did not contain a valid selection")
	}

	schema, ok := request.Selection.Selection.(*fivetran_sdk.Selection_WithSchema)
	if !ok {
		return status.Error(codes.InvalidArgument, "request did not contain a Selection_WithSchema")
	}

	logger := handlers.NewLogger(server, requestId, psc.TreatTinyIntAsBoolean)
	defer logger.Release()
	var db handlers.PlanetScaleDatabase
	if c.clientConstructor == nil {
		mysql, err := handlers.NewMySQL(psc)
		if err != nil {
			return status.Error(codes.InvalidArgument, "unable to open connection to PlanetScale database")
		}
		defer mysql.Close()
		db = handlers.NewEdgeDatabase(&mysql)
	} else {
		db = c.clientConstructor()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	shards, err := db.ListShards(ctx, *psc)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "unable to list shards for this database : %q", err)
	}
	var state *handlers.SyncState
	state, err = handlers.StateFromRequest(request, *psc, shards, *schema)
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
	return c.sync.Handle(psc, &db, logger, state, schema)
}

func newRequestLogger(requestID string) *log.Logger {
	return log.New(os.Stdout, requestID+"  ", log.LstdFlags)
}

func newRequestID() string {
	id := uuid.New()
	return "request-" + id.String()
}
