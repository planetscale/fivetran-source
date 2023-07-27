package handlers

import (
	"context"

	fivetransdk_v2 "github.com/planetscale/fivetran-sdk-grpc/go"
	"github.com/planetscale/fivetran-source/lib"
)

type CheckConnection struct{}

func (CheckConnection) Handle(ctx context.Context, database lib.ConnectClient, s string, source *lib.PlanetScaleSource) (*fivetransdk_v2.TestResponse, error) {
	resp := &fivetransdk_v2.TestResponse{
		Response: &fivetransdk_v2.TestResponse_Success{
			Success: true,
		},
	}
	if err := database.CanConnect(ctx, *source); err != nil {
		resp.Response = &fivetransdk_v2.TestResponse_Failure{
			Failure: err.Error(),
		}
	}

	return resp, nil
}
