package handlers

import (
	"context"

	fivetransdk_v2 "github.com/planetscale/fivetran-sdk-grpc/go"
)

type ConfigurationForm struct{}

const CheckConnectionTestName string = "check_connection"

func (ConfigurationForm) Handle(ctx context.Context, _ *fivetransdk_v2.ConfigurationFormRequest) (*fivetransdk_v2.ConfigurationFormResponse, error) {
	resp := &fivetransdk_v2.ConfigurationFormResponse{
		Fields: []*fivetransdk_v2.FormField{
			{
				Name:  "host",
				Label: "Database host name",
				Type: &fivetransdk_v2.FormField_TextField{
					TextField: fivetransdk_v2.TextField_PlainText,
				},
				Required: true,
			},
			{
				Name:  "database",
				Label: "Database name",
				Type: &fivetransdk_v2.FormField_TextField{
					TextField: fivetransdk_v2.TextField_PlainText,
				},
				Required: true,
			},
			{
				Name:  "username",
				Label: "Database username",
				Type: &fivetransdk_v2.FormField_TextField{
					TextField: fivetransdk_v2.TextField_PlainText,
				},
				Required: true,
			},
			{
				Name:  "password",
				Label: "Database password",
				Type: &fivetransdk_v2.FormField_TextField{
					TextField: fivetransdk_v2.TextField_Password,
				},
				Required: true,
			},
			{
				Name:  "shards",
				Label: "(Optional) Comma-separated list of shards to sync",
				Type: &fivetransdk_v2.FormField_TextField{
					TextField: fivetransdk_v2.TextField_PlainText,
				},
			},
			{
				Name:  "treat_tiny_int_as_boolean",
				Label: "Treat tinyint(1) as boolean",
				Type: &fivetransdk_v2.FormField_DropdownField{
					DropdownField: &fivetransdk_v2.DropdownField{
						DropdownField: []string{
							"true", "false",
						},
					},
				},
			},
		},
		Tests: []*fivetransdk_v2.ConfigurationTest{
			{
				Name:  CheckConnectionTestName,
				Label: "Check connection",
			},
		},
	}

	resp.SchemaSelectionSupported = true
	resp.TableSelectionSupported = true

	return resp, nil
}
