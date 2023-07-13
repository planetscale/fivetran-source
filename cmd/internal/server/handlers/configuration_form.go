package handlers

import (
	"context"

	fivetransdk "github.com/planetscale/fivetran-proto/go"
)

type ConfigurationForm struct{}

const CheckConnectionTestName string = "check_connection"

func (ConfigurationForm) Handle(ctx context.Context, _ *fivetransdk.ConfigurationFormRequest) (*fivetransdk.ConfigurationFormResponse, error) {
	resp := &fivetransdk.ConfigurationFormResponse{
		Fields: []*fivetransdk.FormField{
			{
				Name:  "host",
				Label: "Database host name",
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
				Required: true,
			},
			{
				Name:  "database",
				Label: "Database name",
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
				Required: true,
			},
			{
				Name:  "username",
				Label: "Database username",
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
				Required: true,
			},
			{
				Name:  "password",
				Label: "Database password",
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_Password,
				},
				Required: true,
			},
			{
				Name:  "shards",
				Label: "(Optional) Comma-separated list of shards to sync",
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
			},
			{
				Name:  "treat_tiny_int_as_boolean",
				Label: "Treat tinyint(1) as boolean",
				Type: &fivetransdk.FormField_DropdownField{
					DropdownField: &fivetransdk.DropdownField{
						DropdownField: []string{
							"true", "false",
						},
					},
				},
			},
		},
		Tests: []*fivetransdk.ConnectorTest{
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
