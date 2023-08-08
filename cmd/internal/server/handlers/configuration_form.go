package handlers

import (
	"context"

	fivetransdk "github.com/planetscale/fivetran-sdk-grpc/go"
)

type ConfigurationForm struct{}

const CheckConnectionTestName string = "check_connection"

func (ConfigurationForm) Handle(ctx context.Context, _ *fivetransdk.ConfigurationFormRequest) (*fivetransdk.ConfigurationFormResponse, error) {
	hostDesc := "Hostname to connect to your PlanetScale database"
	dbDesc := "Name of your PlanetScale database"
	usernameDesc := "Username to connect to your PlanetScale database"
	passwordDesc := "Password to connect to your PlanetScale database"
	tinyIntDesc := "Enable this setting to serialize tinyint(1) as boolean values"
	resp := &fivetransdk.ConfigurationFormResponse{
		Fields: []*fivetransdk.FormField{
			{
				Name:        "host",
				Label:       "Database host name",
				Description: &hostDesc,
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
				Required: true,
			},
			{
				Name:        "database",
				Label:       "Database name",
				Description: &dbDesc,
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
				Required: true,
			},
			{
				Name:        "username",
				Label:       "Database username",
				Description: &usernameDesc,
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
				Required: true,
			},
			{
				Name:        "password",
				Label:       "Database password",
				Description: &passwordDesc,
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
				Name:        "treat_tiny_int_as_boolean",
				Label:       "Treat tinyint(1) as boolean",
				Description: &tinyIntDesc,
				Type: &fivetransdk.FormField_DropdownField{
					DropdownField: &fivetransdk.DropdownField{
						DropdownField: []string{
							"true", "false",
						},
					},
				},
			},
		},
		Tests: []*fivetransdk.ConfigurationTest{
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
