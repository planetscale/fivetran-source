package handlers

import (
	"context"

	"github.com/planetscale/connect-sdk/lib"

	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk"
)

type Schema struct{}

func (Schema) Handle(ctx context.Context, psc *lib.PlanetScaleSource, db *lib.MysqlClient) (*fivetransdk.SchemaResponse, error) {
	schemaBuilder := NewSchemaBuilder(psc.TreatTinyIntAsBoolean)
	if err := (*db).BuildSchema(ctx, *psc, schemaBuilder); err != nil {
		return nil, err
	}
	return schemaBuilder.(*FiveTranSchemaBuilder).BuildResponse()
}
