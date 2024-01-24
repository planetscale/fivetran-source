package server

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/fivetran-source/lib"

	"github.com/planetscale/fivetran-source/cmd/internal/server/handlers"
	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk"
)

type ConfigurationFormHandler interface {
	Handle(context.Context, *fivetransdk.ConfigurationFormRequest) (*fivetransdk.ConfigurationFormResponse, error)
}

type CheckConnectionHandler interface {
	Handle(context.Context, lib.ConnectClient, string, *lib.PlanetScaleSource) (*fivetransdk.TestResponse, error)
}
type SchemaHandler interface {
	Handle(context.Context, *lib.PlanetScaleSource, *lib.MysqlClient) (*fivetransdk.SchemaResponse, error)
}

type SyncHandler interface {
	Handle(*lib.PlanetScaleSource, *lib.ConnectClient, handlers.Serializer, *lib.SyncState, *fivetransdk.Selection_WithSchema) error
}

func NewConfigurationFormHandler() ConfigurationFormHandler {
	return &handlers.ConfigurationForm{}
}

func NewSyncHandler() SyncHandler {
	return &handlers.Sync{}
}

func NewSchemaHandler() SchemaHandler {
	return &handlers.Schema{}
}

func NewCheckConnectionHandler() CheckConnectionHandler {
	return &handlers.CheckConnection{}
}

// ConfiguredRequest is a grpc request that contains a Configuration in the payload.
// current examples are : Test, Schema & Update
type ConfiguredRequest interface {
	GetConfiguration() map[string]string
}

// StatefulRequest is a grpc request that contains sync state in the payload.
// current examples are : Update
type StatefulRequest interface {
	GetStateJson() string
}

// SourceFromRequest extracts the required configuration values from the map
// and returns a usable PlanetScaleSource to connect to a PlanetScale database.
func SourceFromRequest(request ConfiguredRequest) (*lib.PlanetScaleSource, error) {
	psc := &lib.PlanetScaleSource{}
	configuration := request.GetConfiguration()
	if val, ok := configuration["username"]; ok {
		psc.Username = val
	} else {
		return nil, errors.New("username not found in configuration")
	}

	if val, ok := configuration["password"]; ok {
		psc.Password = val
	} else {
		return nil, errors.New("password not found in configuration")
	}

	if val, ok := configuration["database"]; ok {
		psc.Database = val
	} else {
		return nil, errors.New("database not found in configuration")
	}

	if val, ok := configuration["host"]; ok {
		psc.Host = val
	} else {
		return nil, errors.New("hostname not found in configuration")
	}

	if val, ok := configuration["treat_tiny_int_as_boolean"]; ok {
		b, err := strconv.ParseBool(val)
		if err != nil {
			return nil, errors.New("treat_tiny_int_as_boolean is not a boolean")
		}
		psc.TreatTinyIntAsBoolean = b
	}

	if val, ok := configuration["use_replica"]; ok {
		b, err := strconv.ParseBool(val)
		if err != nil {
			return nil, errors.New("use_replica is not a boolean")
		}
		psc.UseReplica = b
	}

	return psc, nil
}

// StateFromRequest unmarshals the stateJson saved in Fivetran
// and turns that into a structure we can use within the connector to
// incrementally sync tables from PlanetScale.
func StateFromRequest(request StatefulRequest, source lib.PlanetScaleSource, shards []string, schemaSelection fivetransdk.Selection_WithSchema) (*lib.SyncState, error) {
	syncState := lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{},
	}

	state := request.GetStateJson()
	if state != "" {
		err := json.Unmarshal([]byte(state), &syncState)
		if err != nil {
			return nil, err
		}
	}

	for _, s := range schemaSelection.WithSchema.Schemas {
		if !s.Included {
			continue
		}
		keyspaceState, ok := syncState.Keyspaces[s.SchemaName]
		if !ok {
			keyspaceState = lib.KeyspaceState{
				Streams: map[string]lib.ShardStates{},
			}
		}
		for _, t := range s.Tables {
			if !t.Included {
				continue
			}
			stateKey := s.SchemaName + ":" + t.TableName
			// if no table cursor was found in the state, or we want to ignore the current cursor,
			// Send along an empty cursor for each shard.
			if _, ok := keyspaceState.Streams[stateKey]; !ok {
				initialState, err := source.GetInitialState(s.SchemaName, shards)
				if err != nil {
					return &syncState, err
				}
				keyspaceState.Streams[stateKey] = initialState
			}
			streamState := keyspaceState.Streams[stateKey]
			// does this streamState have values for all shards?
			// if not, fill them in with initial state.
			for _, shard := range shards {
				if _, ok := streamState.Shards[shard]; !ok {
					streamState.Shards[shard], _ = lib.TableCursorToSerializedCursor(&psdbconnect.TableCursor{
						Shard:    shard,
						Keyspace: s.SchemaName,
						Position: "",
					})
				}
			}
		}
		syncState.Keyspaces[s.SchemaName] = keyspaceState
	}

	return &syncState, nil
}
