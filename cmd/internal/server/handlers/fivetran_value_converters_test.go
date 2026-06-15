package handlers

import (
	"math"
	"testing"

	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

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

func TestLongConverter_BitValues(t *testing.T) {
	converter, err := GetConverter(fivetransdk.DataType_LONG)
	require.NoError(t, err)

	tests := []struct {
		name string
		raw  []byte
		want int64
	}{
		{name: "zero", raw: []byte{0x00}, want: 0},
		{name: "one", raw: []byte{0x01}, want: 1},
		{name: "two", raw: []byte{0x02}, want: 2},
		{name: "max seven bit", raw: []byte{0x7f}, want: 127},
		{name: "eight bit high bit", raw: []byte{0x80}, want: 128},
		{name: "eight bit max", raw: []byte{0xff}, want: 255},
		{name: "two byte value", raw: []byte{0x01, 0x00}, want: 256},
		{name: "max int64", raw: []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, want: math.MaxInt64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter(sqltypes.MakeTrusted(querypb.Type_BIT, tt.raw))
			require.NoError(t, err)

			got, ok := result.Inner.(*fivetransdk.ValueType_Long)
			require.True(t, ok)
			assert.Equal(t, tt.want, got.Long)
		})
	}
}

func TestLongConverter_BitOverflow(t *testing.T) {
	converter, err := GetConverter(fivetransdk.DataType_LONG)
	require.NoError(t, err)

	_, err = converter(sqltypes.MakeTrusted(querypb.Type_BIT, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows int64")

	_, err = converter(sqltypes.MakeTrusted(querypb.Type_BIT, []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "uses 9 bytes")
}
