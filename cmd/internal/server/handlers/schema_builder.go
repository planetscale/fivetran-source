package handlers

import (
	"strings"

	"github.com/planetscale/fivetran-source/lib"

	fivetransdk "github.com/planetscale/fivetran-proto/go"
)

type fivetranSchemaBuilder struct {
	schemas               map[string]*fivetransdk.Schema
	tables                map[string]map[string]*fivetransdk.Table
	treatTinyIntAsBoolean bool
}

func NewSchemaBuilder(treatTinyIntAsBoolean bool) lib.SchemaBuilder {
	return &fivetranSchemaBuilder{
		treatTinyIntAsBoolean: treatTinyIntAsBoolean,
	}
}

func (s *fivetranSchemaBuilder) OnKeyspace(keyspaceName string) {
	schema := &fivetransdk.Schema{
		Name:   keyspaceName,
		Tables: []*fivetransdk.Table{},
	}
	if s.schemas == nil {
		s.schemas = map[string]*fivetransdk.Schema{}
	}

	s.schemas[keyspaceName] = schema
}

func (s *fivetranSchemaBuilder) OnTable(keyspaceName, tableName string) {
	s.getOrCreateTable(keyspaceName, tableName)
}

func (s *fivetranSchemaBuilder) getOrCreateTable(keyspaceName string, tableName string) *fivetransdk.Table {
	_, ok := s.schemas[keyspaceName]
	if !ok {
		s.OnKeyspace(keyspaceName)
	}
	var table *fivetransdk.Table
	schema, ok := s.schemas[keyspaceName]
	if !ok {
		panic("schema not found " + keyspaceName)
	}
	for _, t := range schema.Tables {
		if t.Name == tableName {
			table = t
		}
	}

	if table == nil {
		table = &fivetransdk.Table{
			Name:    tableName,
			Columns: []*fivetransdk.Column{},
		}
		schema.Tables = append(schema.Tables, table)
	}

	if s.tables == nil {
		s.tables = map[string]map[string]*fivetransdk.Table{}
	}

	if s.tables[keyspaceName] == nil {
		s.tables[keyspaceName] = map[string]*fivetransdk.Table{}
	}

	s.tables[keyspaceName][tableName] = table
	return table
}

func (s *fivetranSchemaBuilder) OnColumns(keyspaceName, tableName string, columns []lib.MysqlColumn) {
	table := s.getOrCreateTable(keyspaceName, tableName)
	if table.Columns == nil {
		table.Columns = []*fivetransdk.Column{}
	}

	for _, column := range columns {
		table.Columns = append(table.Columns, &fivetransdk.Column{
			Name:       column.Name,
			Type:       getFivetranDataType(column.Type, s.treatTinyIntAsBoolean),
			PrimaryKey: column.IsPrimaryKey,
		})
	}
}

func (s *fivetranSchemaBuilder) BuildResponse() (*fivetransdk.SchemaResponse, error) {
	responseSchema := &fivetransdk.SchemaResponse_WithSchema{
		WithSchema: &fivetransdk.SchemaList{},
	}

	for _, schema := range s.schemas {
		responseSchema.WithSchema.Schemas = append(responseSchema.WithSchema.Schemas, schema)
	}

	resp := &fivetransdk.SchemaResponse{
		Response: responseSchema,
	}

	return resp, nil
}

// Convert columnType to fivetran type
func getFivetranDataType(mType string, treatTinyIntAsBoolean bool) fivetransdk.DataType {
	mysqlType := strings.ToLower(mType)
	if strings.HasPrefix(mysqlType, "tinyint") {
		if treatTinyIntAsBoolean {
			return fivetransdk.DataType_BOOLEAN
		}

		return fivetransdk.DataType_INT
	}

	// A BIT(n) type can have a length of 1 to 64
	// we serialize these as a LONG : int64 value when serializing rows.
	if strings.HasPrefix(mysqlType, "bit") {
		return fivetransdk.DataType_LONG
	}

	if strings.HasPrefix(mysqlType, "varbinary") {
		return fivetransdk.DataType_BINARY
	}

	if strings.HasPrefix(mysqlType, "int") {
		if strings.Contains(mysqlType, "unsigned") {
			return fivetransdk.DataType_LONG
		}
		return fivetransdk.DataType_INT
	}

	if strings.HasPrefix(mysqlType, "smallint") {
		return fivetransdk.DataType_INT
	}

	if strings.HasPrefix(mysqlType, "bigint") {
		return fivetransdk.DataType_LONG
	}

	if strings.HasPrefix(mysqlType, "decimal") {
		return fivetransdk.DataType_DECIMAL
	}

	if strings.HasPrefix(mysqlType, "double") ||
		strings.HasPrefix(mysqlType, "float") {
		return fivetransdk.DataType_DOUBLE
	}

	if strings.HasPrefix(mysqlType, "timestamp") {
		return fivetransdk.DataType_UTC_DATETIME
	}

	if strings.HasPrefix(mysqlType, "time") {
		return fivetransdk.DataType_STRING
	}

	if strings.HasPrefix(mysqlType, "datetime") {
		return fivetransdk.DataType_NAIVE_DATETIME
	}

	if strings.HasPrefix(mysqlType, "year") {
		return fivetransdk.DataType_INT
	}

	if strings.HasPrefix(mysqlType, "varchar") ||
		strings.HasPrefix(mysqlType, "text") ||
		strings.HasPrefix(mysqlType, "enum") ||
		strings.HasPrefix(mysqlType, "char") {
		return fivetransdk.DataType_STRING
	}

	if strings.HasPrefix(mysqlType, "set") ||
		strings.HasPrefix(mysqlType, "geometry") ||
		strings.HasPrefix(mysqlType, "geometrycollection") ||
		strings.HasPrefix(mysqlType, "multipoint") ||
		strings.HasPrefix(mysqlType, "multipolygon") ||
		strings.HasPrefix(mysqlType, "polygon") ||
		strings.HasPrefix(mysqlType, "point") ||
		strings.HasPrefix(mysqlType, "linestring") ||
		strings.HasPrefix(mysqlType, "multilinestring") {
		return fivetransdk.DataType_JSON
	}

	switch mysqlType {
	case "date":
		return fivetransdk.DataType_NAIVE_DATE
	case "json":
		return fivetransdk.DataType_JSON
	case "tinytext":
		return fivetransdk.DataType_STRING
	case "mediumtext":
		return fivetransdk.DataType_STRING
	case "longtext":
		return fivetransdk.DataType_STRING
	case "binary":
		return fivetransdk.DataType_BINARY
	case "blob":
		return fivetransdk.DataType_BINARY
	case "longblob":
		return fivetransdk.DataType_BINARY
	case "mediumblob":
		return fivetransdk.DataType_BINARY
	case "tinyblob":
		return fivetransdk.DataType_BINARY
	case "bit":
		return fivetransdk.DataType_BOOLEAN
	case "time":
		return fivetransdk.DataType_STRING
	default:
		return fivetransdk.DataType_STRING
	}
}
