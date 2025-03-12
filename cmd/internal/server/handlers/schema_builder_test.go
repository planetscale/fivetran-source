package handlers

import (
	"fmt"
	"testing"

	"github.com/planetscale/fivetran-source/lib"

	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"github.com/stretchr/testify/assert"
)

func TestCanIgnoreVitessGCTables(t *testing.T) {
	sb := NewSchemaBuilder(true)
	sb.OnKeyspace("Employees")

	sb.OnTable("Employees", "_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410")
	sb.OnTable("Employees", "_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410")
	sb.OnTable("Employees", "_vt_EVAC_6ace8bcef73211ea87e9f875a4d24e90_20200915120410")
	sb.OnTable("Employees", "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410")
	sb.OnTable("Employees", "_750a3e1f_e6f3_5249_82af_82f5d325ecab_20240528153135_vrepl")
	resp, err := sb.(*FiveTranSchemaBuilder).BuildResponse()
	assert.NoError(t, err)
	schemaResponse, ok := resp.Response.(*fivetransdk.SchemaResponse_WithSchema)
	assert.True(t, ok)
	assert.NotNil(t, schemaResponse)
	assert.Len(t, schemaResponse.WithSchema.Schemas, 1)
	assert.Len(t, schemaResponse.WithSchema.Schemas[0].Tables, 0)
}

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
	resp, err := sb.(*FiveTranSchemaBuilder).BuildResponse()
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

func TestCanBuildUpdateSchema(t *testing.T) {
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
		{
			Name: "dept_category",
			Type: "enum('hr','it','sales','engineering')",
		},
		{
			Name: "dept_locations",
			Type: "set('san francisco','new york','seattle','boston')",
		},
	})
	resp, err := sb.(*FiveTranSchemaBuilder).BuildUpdateResponse()
	assert.NoError(t, err)
	schemaResponse := resp.SchemaList
	assert.NotNil(t, schemaResponse)
	assert.Len(t, schemaResponse.Schemas, 1)
	assert.Len(t, schemaResponse.Schemas[0].Tables, 1)
	table := schemaResponse.Schemas[0].Tables[0]
	assert.Equal(t, "departments", table.Name)
	assert.Len(t, table.Columns, 4)
	assert.Equal(t, "dept_no", table.Columns[0].Name)
	assert.True(t, table.Columns[0].PrimaryKey)
	assert.Equal(t, "dept_name", table.Columns[1].Name)
	assert.False(t, table.Columns[1].PrimaryKey)
	assert.Equal(t, table.Columns[2].Name, "dept_category")
	assert.Equal(t, table.Columns[3].Name, "dept_locations")

	enumsAndSets := resp.EnumsAndSets
	assert.NotNil(t, enumsAndSets)
	assert.Equal(t, enumsAndSets["Employees"]["departments"]["dept_category"].values, []string{"hr", "it", "sales", "engineering"})
	assert.Equal(t, enumsAndSets["Employees"]["departments"]["dept_category"].columnType, "enum")
	assert.Equal(t, enumsAndSets["Employees"]["departments"]["dept_locations"].values, []string{"san francisco", "new york", "seattle", "boston"})
	assert.Equal(t, enumsAndSets["Employees"]["departments"]["dept_locations"].columnType, "set")
}

func TestSchema_CanPickRightFivetranType(t *testing.T) {
	tests := []struct {
		MysqlType             string
		FivetranType          fivetransdk.DataType
		TreatTinyIntAsBoolean bool
		DecimalParams         *fivetransdk.DecimalParams
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
			MysqlType:             "tinyint unsigned",
			FivetranType:          fivetransdk.DataType_INT,
			TreatTinyIntAsBoolean: true,
		},
		{
			MysqlType:             "tinyint",
			FivetranType:          fivetransdk.DataType_INT,
			TreatTinyIntAsBoolean: true,
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
			MysqlType:    "varbinary",
			FivetranType: fivetransdk.DataType_BINARY,
		},
		{
			MysqlType:    "binary(16)",
			FivetranType: fivetransdk.DataType_BINARY,
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
			DecimalParams: &fivetransdk.DecimalParams{
				Precision: 12,
				Scale:     5,
			},
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
			p, d := getFivetranDataType(typeTest.MysqlType, typeTest.TreatTinyIntAsBoolean)
			assert.Equal(t, typeTest.FivetranType, p)
			if typeTest.DecimalParams != nil {
				assert.Equal(t, typeTest.DecimalParams, d)
			}
		})
	}
}

func TestCanDetectDecimalPrecision(t *testing.T) {
	tests := []struct {
		MysqlType string
		Precision uint32
		Scale     uint32
	}{
		{
			MysqlType: "Decimal",
			Precision: 10,
			Scale:     0,
		},
		{
			MysqlType: "Decimal(32)",
			Precision: 32,
			Scale:     0,
		},
		{
			MysqlType: "Decimal(32,4)",
			Precision: 32,
			Scale:     4,
		},
	}
	for _, typeTest := range tests {
		t.Run(fmt.Sprintf("decimal_precision%v", typeTest.MysqlType), func(t *testing.T) {
			p := getDecimalParams(typeTest.MysqlType)
			assert.Equal(t, typeTest.Precision, p.Precision)
			assert.Equal(t, typeTest.Scale, p.Scale)
		})
	}
}
