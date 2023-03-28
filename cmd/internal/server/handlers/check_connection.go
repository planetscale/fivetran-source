package handlers

import (
	"context"

	fivetransdk "github.com/planetscale/fivetran-proto/proto/fivetransdk/v1alpha1"
)

type CheckConnection struct{}

func (CheckConnection) Handle(ctx context.Context, database PlanetScaleDatabase, s string, source *PlanetScaleSource) (*fivetransdk.TestResponse, error) {
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
