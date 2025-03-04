package handlers

import (
	"context"

	"github.com/planetscale/fivetran-source/lib"

	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
)

type CheckConnection struct{}

func (CheckConnection) Handle(ctx context.Context, database lib.ConnectClient, s string, source *lib.PlanetScaleSource) (*fivetransdk.TestResponse, error) {
	resp := &fivetransdk.TestResponse{
		Response: &fivetransdk.TestResponse_Success{
			Success: true,
		},
	}
	if err := database.CanConnect(ctx, *source); err != nil {
		resp.Response = &fivetransdk.TestResponse_Failure{
			Failure: err.Error(),
		}
	}

	return resp, nil
}
