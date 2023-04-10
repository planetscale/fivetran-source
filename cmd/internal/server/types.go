package server

import (
	"context"
	"github.com/planetscale/fivetran-source/lib"

	fivetransdk "github.com/planetscale/fivetran-proto/proto/fivetransdk/v1alpha1"
	"github.com/planetscale/fivetran-source/cmd/internal/server/handlers"
)

type ConfigurationFormHandler interface {
	Handle(context.Context, *fivetransdk.ConfigurationFormRequest) (*fivetransdk.ConfigurationFormResponse, error)
}

type CheckConnectionHandler interface {
	Handle(context.Context, lib.PlanetScaleDatabase, string, *lib.PlanetScaleSource) (*fivetransdk.TestResponse, error)
}
type SchemaHandler interface {
	Handle(context.Context, *lib.PlanetScaleSource, *lib.PlanetScaleEdgeMysqlAccess) (*fivetransdk.SchemaResponse, error)
}

type SyncHandler interface {
	Handle(*lib.PlanetScaleSource, *lib.PlanetScaleDatabase, handlers.Logger, *lib.SyncState, *fivetransdk.Selection_WithSchema) error
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
