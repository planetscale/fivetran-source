package handlers

import (
	"testing"

	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
)

func TestSetConverter_BitmaskValues(t *testing.T) {
	converter, err := GetSetConverter([]string{"read", "write", "delete", "admin"})
	require.NoError(t, err)

	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "empty set", value: "0", want: ""},
		{name: "first value", value: "1", want: "read"},
		{name: "first two values", value: "3", want: "read,write"},
		{name: "fourth value", value: "8", want: "admin"},
		{name: "non numeric string value", value: "read,write", want: "read,write"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter(sqltypes.NewVarChar(tt.value))
			require.NoError(t, err)
			assert.Equal(t, tt.want, result.GetJson())
		})
	}
}

func TestSetConverter_ParsesUint64Masks(t *testing.T) {
	setValues := make([]string, 64)
	setValues[63] = "slot64"
	converter, err := GetSetConverter(setValues)
	require.NoError(t, err)

	result, err := converter(sqltypes.NewVarChar("9223372036854775808"))
	require.NoError(t, err)
	assert.Equal(t, "slot64", result.GetJson())
}

func TestSetConverter_UnmappedBitReturnsError(t *testing.T) {
	converter, err := GetSetConverter([]string{"read", "write"})
	require.NoError(t, err)

	_, err = converter(sqltypes.NewVarChar("4"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bit 3 has no mapped value")
}

func TestJSONConverter_EmptyString(t *testing.T) {
	converter, err := GetConverter(fivetransdk.DataType_JSON)
	require.NoError(t, err)

	// Empty string should convert to NULL for BigQuery compatibility
	emptyValue := sqltypes.NewVarChar("")
	result, err := converter(emptyValue)
	require.NoError(t, err)
	require.NotNil(t, result)

	_, ok := result.Inner.(*fivetransdk.ValueType_Null)
	assert.True(t, ok, "empty JSON string should convert to NULL")
}

func TestJSONConverter_ValidJSON(t *testing.T) {
	converter, err := GetConverter(fivetransdk.DataType_JSON)
	require.NoError(t, err)

	// Valid JSON should pass through
	jsonValue := sqltypes.NewVarChar(`{"key": "value"}`)
	result, err := converter(jsonValue)
	require.NoError(t, err)
	require.NotNil(t, result)

	json, ok := result.Inner.(*fivetransdk.ValueType_Json)
	require.True(t, ok)
	assert.Equal(t, `{"key": "value"}`, json.Json)
}
