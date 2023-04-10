package handlers

import (
	"context"

	fivetransdk "github.com/planetscale/fivetran-proto/go"
)

type Schema struct{}

func (Schema) Handle(ctx context.Context, psc *PlanetScaleSource, db *PlanetScaleDatabase) (*fivetransdk.SchemaResponse, error) {
	return (*db).DiscoverSchema(ctx, *psc)
}
