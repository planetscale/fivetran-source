package handlers

import (
	"context"

	"github.com/planetscale/fivetran-source/lib"

	fivetransdk "github.com/planetscale/fivetran-proto/proto/fivetransdk/v1alpha1"
)

type Schema struct{}

func (Schema) Handle(ctx context.Context, psc *lib.PlanetScaleSource, db *lib.PlanetScaleEdgeMysqlAccess) (*fivetransdk.SchemaResponse, error) {
	schemaBuilder := NewSchemaBuilder()
	if err := (*db).BuildSchema(ctx, *psc, schemaBuilder); err != nil {
		return nil, err
	}

	return schemaBuilder.(*fivetranSchemaBuilder).BuildResponse()
}
