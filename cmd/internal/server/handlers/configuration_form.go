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
				Label: "The host name of the database",
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
				Required: true,
			},
			{
				Name:  "database",
				Label: "The PlanetScale database name",
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
				Required: true,
			},
			{
				Name:  "username",
				Label: "The username which is used to access the database",
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
				Required: true,
			},
			{
				Name:  "password",
				Label: "The password associated with the username",
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_Password,
				},
				Required: true,
			},
			{
				Name:  "shards",
				Label: "A comma-separated list of shards you'd like to sync; by default all shards are synced",
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
			},
			{
				Name:  "do_not_treat_tiny_int_as_boolean",
				Label: "Do not treat tinyint(1) as boolean",
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
