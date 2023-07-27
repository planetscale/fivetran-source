package handlers

import (
	"context"

	fivetransdk_v2 "github.com/planetscale/fivetran-sdk-grpc/go"
	"github.com/planetscale/fivetran-source/lib"
)

type Schema struct{}

func (Schema) Handle(ctx context.Context, psc *lib.PlanetScaleSource, db *lib.MysqlClient) (*fivetransdk_v2.SchemaResponse, error) {
	schemaBuilder := NewSchemaBuilder(psc.TreatTinyIntAsBoolean)
	if err := (*db).BuildSchema(ctx, *psc, schemaBuilder); err != nil {
		return nil, err
	}

	return schemaBuilder.(*fivetranSchemaBuilder).BuildResponse()
}
