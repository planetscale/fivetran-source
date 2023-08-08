package handlers

import (
	"context"
	"fmt"

	"github.com/planetscale/fivetran-source/lib"

	fivetransdk "github.com/planetscale/fivetran-sdk-grpc/go"
)

type Schema struct{}

func (Schema) Handle(ctx context.Context, psc *lib.PlanetScaleSource, db *lib.MysqlClient) (*fivetransdk.SchemaResponse, error) {
	schemaBuilder := NewSchemaBuilder(psc.TreatTinyIntAsBoolean)
	if err := (*db).BuildSchema(ctx, *psc, schemaBuilder); err != nil {
		return nil, err
	}
	r, _ := schemaBuilder.(*fivetranSchemaBuilder).BuildResponse()
	fmt.Printf("\n\t tables are:  %v ", r.Response.(*fivetransdk.SchemaResponse_WithSchema).WithSchema.Schemas[0].Tables)
	return r, nil
}
