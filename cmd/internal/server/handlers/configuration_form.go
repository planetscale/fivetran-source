package handlers

import (
	"context"

	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
)

type ConfigurationForm struct{}

const CheckConnectionTestName string = "check_connection"

func (ConfigurationForm) Handle(ctx context.Context, _ *fivetransdk.ConfigurationFormRequest) (*fivetransdk.ConfigurationFormResponse, error) {
	hostDesc := "Hostname to connect to your PlanetScale database"
	dbDesc := "Name of your PlanetScale database"
	usernameDesc := "Username to connect to your PlanetScale database"
	passwordDesc := "Password to connect to your PlanetScale database"
	tinyIntDesc := "Enable this setting to serialize tinyint(1) as boolean values"
	useReplicaDesc := "Only set to true if your PlanetScale branch has a replica. PlanetScale Development branches do not have replicas."
	autoResyncDesc := "When a schema change leaves the saved position unreadable, automatically reset the cursor and run a historical sync for the affected table instead of failing the sync and waiting for a manual re-sync. Defaults to false; enabling it trades additional monthly active rows for unattended recovery."
	propagateNewColumnsDesc := "EXPERIMENTAL - leave disabled unless you have been asked to turn it on. " +
		"When enabled, a column added to a table after this connection was set up starts syncing on its own, " +
		"instead of requiring a historical re-sync; a dropped column stops being requested. " +
		"This only applies to tables where you have allowed new columns in the connection's schema settings. " +
		"Defaults to false. If a sync behaves unexpectedly after enabling this, turn it off and re-sync."
	required := true
	resp := &fivetransdk.ConfigurationFormResponse{
		Fields: []*fivetransdk.FormField{
			{
				Name:        "host",
				Label:       "Database host name",
				Description: &hostDesc,
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
				Required: &required,
			},
			{
				Name:        "database",
				Label:       "Database name",
				Description: &dbDesc,
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
				Required: &required,
			},
			{
				Name:        "username",
				Label:       "Database username",
				Description: &usernameDesc,
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
				Required: &required,
			},
			{
				Name:        "password",
				Label:       "Database password",
				Description: &passwordDesc,
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_Password,
				},
				Required: &required,
			},
			{
				Name:  "shards",
				Label: "Comma-separated list of shards to sync",
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
				},
			},
			{
				Name:        "use_replica",
				Label:       "Use Replica?",
				Description: &useReplicaDesc,
				Type: &fivetransdk.FormField_DropdownField{
					DropdownField: &fivetransdk.DropdownField{
						DropdownField: []string{
							"true", "false",
						},
					},
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
			{
				Name:        "auto_resync_on_schema_change",
				Label:       "Automatically re-sync after a schema change?",
				Description: &autoResyncDesc,
				Type: &fivetransdk.FormField_DropdownField{
					DropdownField: &fivetransdk.DropdownField{
						DropdownField: []string{
							"true", "false",
						},
					},
				},
			},
			{
				Name:        "propagate_new_columns",
				Label:       "[Experimental] Adapt to added and dropped columns automatically?",
				Description: &propagateNewColumnsDesc,
				Type: &fivetransdk.FormField_DropdownField{
					DropdownField: &fivetransdk.DropdownField{
						DropdownField: []string{
							"true", "false",
						},
					},
				},
			},
			{
				Name:        "starting_gtids",
				Label:       "JSON containing keyspace, shard, and starting GTIDs",
				Description: &tinyIntDesc,
				Type: &fivetransdk.FormField_TextField{
					TextField: fivetransdk.TextField_PlainText,
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
