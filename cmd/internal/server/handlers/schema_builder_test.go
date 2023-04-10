package handlers

import (
	"fmt"
	"testing"

	fivetransdk "github.com/planetscale/fivetran-proto/proto/fivetransdk/v1alpha1"
	"github.com/stretchr/testify/assert"
)

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
