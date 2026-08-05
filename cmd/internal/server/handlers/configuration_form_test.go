package handlers

import (
	"context"
	"strings"
	"testing"

	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// propagate_new_columns is experimental, so the form has to say so plainly and
// must not present itself as a safe default.
func TestConfigurationForm_PropagateNewColumnsIsOfferedAsExperimental(t *testing.T) {
	resp, err := ConfigurationForm{}.Handle(context.Background(), &fivetransdk.ConfigurationFormRequest{})
	require.NoError(t, err)

	var field *fivetransdk.FormField
	for _, f := range resp.Fields {
		if f.Name == "propagate_new_columns" {
			field = f
		}
	}

	require.NotNil(t, field, "propagate_new_columns must be offered on the setup form")
	assert.Contains(t, strings.ToLower(field.Label), "experimental", "the label must flag the field as experimental")
	require.NotNil(t, field.Description)
	assert.Contains(t, strings.ToLower(*field.Description), "experimental")

	dropdown, ok := field.Type.(*fivetransdk.FormField_DropdownField)
	require.Truef(t, ok, "expected a dropdown, got %T", field.Type)
	assert.ElementsMatch(t, []string{"true", "false"}, dropdown.DropdownField.DropdownField)

	// Nothing marks it required -- an untouched form must leave it off.
	assert.True(t, field.Required == nil || !*field.Required)
}
