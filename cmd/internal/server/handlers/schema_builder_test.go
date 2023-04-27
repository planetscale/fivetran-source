package handlers

import (
	"fmt"
	"testing"

	"github.com/planetscale/fivetran-source/lib"

	fivetransdk "github.com/planetscale/fivetran-proto/go"
	"github.com/stretchr/testify/assert"
)

func TestCanBuildSchema(t *testing.T) {
	sb := NewSchemaBuilder(true)
	sb.OnKeyspace("Employees")
	sb.OnTable("Employees", "departments")
	sb.OnColumns("Employees", "departments", []lib.MysqlColumn{
		{
			Name:         "dept_no",
			Type:         "varchar(40)",
			IsPrimaryKey: true,
		},
		{
			Name: "dept_name",
			Type: "varchar(400)",
		},
	})
	resp, err := sb.(*fivetranSchemaBuilder).BuildResponse()
	assert.NoError(t, err)
	schemaResponse, ok := resp.Response.(*fivetransdk.SchemaResponse_WithSchema)
	assert.True(t, ok)
	assert.NotNil(t, schemaResponse)
	assert.Len(t, schemaResponse.WithSchema.Schemas, 1)
	assert.Len(t, schemaResponse.WithSchema.Schemas[0].Tables, 1)
	table := schemaResponse.WithSchema.Schemas[0].Tables[0]
	assert.Equal(t, "departments", table.Name)
	assert.Len(t, table.Columns, 2)
	assert.Equal(t, "dept_no", table.Columns[0].Name)
	assert.True(t, table.Columns[0].PrimaryKey)
	assert.Equal(t, "dept_name", table.Columns[1].Name)
	assert.False(t, table.Columns[1].PrimaryKey)
}

func TestSchema_CanPickRightFivetranType(t *testing.T) {
	tests := []struct {
		MysqlType             string
		FivetranType          fivetransdk.DataType
		TreatTinyIntAsBoolean bool
	}{
		{
			MysqlType:    "int(32)",
			FivetranType: fivetransdk.DataType_INT,
		},
		{
			MysqlType:    "int unsigned",
			FivetranType: fivetransdk.DataType_LONG,
		},
		{
			MysqlType:             "tinyint(1)",
			FivetranType:          fivetransdk.DataType_BOOLEAN,
			TreatTinyIntAsBoolean: true,
		},
		{
			MysqlType:             "tinyint(1)",
			FivetranType:          fivetransdk.DataType_INT,
			TreatTinyIntAsBoolean: false,
		},
		{
			MysqlType:             "timestamp",
			FivetranType:          fivetransdk.DataType_UTC_DATETIME,
			TreatTinyIntAsBoolean: true,
		},
		{
			MysqlType:             "time",
			FivetranType:          fivetransdk.DataType_STRING,
			TreatTinyIntAsBoolean: true,
		},
		{
			MysqlType:    "bigint(16)",
			FivetranType: fivetransdk.DataType_LONG,
		},
		{
			MysqlType:    "bigint unsigned",
			FivetranType: fivetransdk.DataType_LONG,
		},
		{
			MysqlType:    "bigint zerofill",
			FivetranType: fivetransdk.DataType_LONG,
		},
		{
			MysqlType:    "datetime(3)",
			FivetranType: fivetransdk.DataType_NAIVE_DATETIME,
		},
		{
			MysqlType:    "date",
			FivetranType: fivetransdk.DataType_NAIVE_DATE,
		},
		{
			MysqlType:    "text",
			FivetranType: fivetransdk.DataType_STRING,
		},
		{
			MysqlType:    "varchar(256)",
			FivetranType: fivetransdk.DataType_STRING,
		},
		{
			MysqlType:    "decimal(12,5)",
			FivetranType: fivetransdk.DataType_DECIMAL,
		},
		{
			MysqlType:    "double",
			FivetranType: fivetransdk.DataType_DOUBLE,
		},
		{
			MysqlType:    "enum",
			FivetranType: fivetransdk.DataType_STRING,
		},
	}

	for _, typeTest := range tests {
		t.Run(fmt.Sprintf("mysql_type_%v", typeTest.MysqlType), func(t *testing.T) {
			p := getFivetranDataType(typeTest.MysqlType, typeTest.TreatTinyIntAsBoolean)
			assert.Equal(t, typeTest.FivetranType, p)
		})
	}
}
