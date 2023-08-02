package handlers

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/planetscale/fivetran-source/lib"

	fivetransdk_v2 "github.com/planetscale/fivetran-sdk-grpc/go"
)

type fivetranSchemaBuilder struct {
	schemas               map[string]*fivetransdk_v2.Schema
	tables                map[string]map[string]*fivetransdk_v2.Table
	treatTinyIntAsBoolean bool
}

func NewSchemaBuilder(treatTinyIntAsBoolean bool) lib.SchemaBuilder {
	return &fivetranSchemaBuilder{
		treatTinyIntAsBoolean: treatTinyIntAsBoolean,
	}
}

func (s *fivetranSchemaBuilder) OnKeyspace(keyspaceName string) {
	schema := &fivetransdk_v2.Schema{
		Name:   keyspaceName,
		Tables: []*fivetransdk_v2.Table{},
	}
	if s.schemas == nil {
		s.schemas = map[string]*fivetransdk_v2.Schema{}
	}

	s.schemas[keyspaceName] = schema
}

func (s *fivetranSchemaBuilder) OnTable(keyspaceName, tableName string) {
	s.getOrCreateTable(keyspaceName, tableName)
}

func (s *fivetranSchemaBuilder) getOrCreateTable(keyspaceName string, tableName string) *fivetransdk_v2.Table {
	_, ok := s.schemas[keyspaceName]
	if !ok {
		s.OnKeyspace(keyspaceName)
	}
	var table *fivetransdk_v2.Table
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
		table = &fivetransdk_v2.Table{
			Name:    tableName,
			Columns: []*fivetransdk_v2.Column{},
		}
		schema.Tables = append(schema.Tables, table)
	}

	if s.tables == nil {
		s.tables = map[string]map[string]*fivetransdk_v2.Table{}
	}

	if s.tables[keyspaceName] == nil {
		s.tables[keyspaceName] = map[string]*fivetransdk_v2.Table{}
	}

	s.tables[keyspaceName][tableName] = table
	return table
}

func (s *fivetranSchemaBuilder) OnColumns(keyspaceName, tableName string, columns []lib.MysqlColumn) {
	table := s.getOrCreateTable(keyspaceName, tableName)
	if table.Columns == nil {
		table.Columns = []*fivetransdk_v2.Column{}
	}

	for _, column := range columns {
		dataType, decimalParams := getFivetranDataType(column.Type, s.treatTinyIntAsBoolean)
		table.Columns = append(table.Columns, &fivetransdk_v2.Column{
			Name:       column.Name,
			Type:       dataType,
			Decimal:    decimalParams,
			PrimaryKey: column.IsPrimaryKey,
		})
	}
}

func (s *fivetranSchemaBuilder) BuildResponse() (*fivetransdk_v2.SchemaResponse, error) {
	responseSchema := &fivetransdk_v2.SchemaResponse_WithSchema{
		WithSchema: &fivetransdk_v2.SchemaList{},
	}

	for _, schema := range s.schemas {
		responseSchema.WithSchema.Schemas = append(responseSchema.WithSchema.Schemas, schema)
	}

	resp := &fivetransdk_v2.SchemaResponse{
		Response: responseSchema,
	}

	return resp, nil
}

// Convert columnType to fivetran type
func getFivetranDataType(mType string, treatTinyIntAsBoolean bool) (fivetransdk_v2.DataType, *fivetransdk_v2.DecimalParams) {
	mysqlType := strings.ToLower(mType)
	if strings.HasPrefix(mysqlType, "tinyint") {
		if treatTinyIntAsBoolean {
			return fivetransdk_v2.DataType_BOOLEAN, nil
		}

		return fivetransdk_v2.DataType_INT, nil
	}

	// Fivetran maps BIT to BOOLEAN
	// See https://fivetran.com/docs/databases/mysql
	if strings.HasPrefix(mysqlType, "bit") {
		return fivetransdk_v2.DataType_BOOLEAN, nil
	}

	if strings.HasPrefix(mysqlType, "varbinary") {
		return fivetransdk_v2.DataType_BINARY, nil
	}

	if strings.HasPrefix(mysqlType, "int") {
		if strings.Contains(mysqlType, "unsigned") {
			return fivetransdk_v2.DataType_LONG, nil
		}
		return fivetransdk_v2.DataType_INT, nil
	}

	if strings.HasPrefix(mysqlType, "smallint") {
		return fivetransdk_v2.DataType_INT, nil
	}

	if strings.HasPrefix(mysqlType, "bigint") {
		return fivetransdk_v2.DataType_LONG, nil
	}

	if strings.HasPrefix(mysqlType, "decimal") {
		return fivetransdk_v2.DataType_DECIMAL, getDecimalParams(mysqlType)
	}

	if strings.HasPrefix(mysqlType, "double") ||
		strings.HasPrefix(mysqlType, "float") {
		return fivetransdk_v2.DataType_DOUBLE, nil
	}

	if strings.HasPrefix(mysqlType, "timestamp") {
		return fivetransdk_v2.DataType_UTC_DATETIME, nil
	}

	if strings.HasPrefix(mysqlType, "time") {
		return fivetransdk_v2.DataType_STRING, nil
	}

	if strings.HasPrefix(mysqlType, "datetime") {
		return fivetransdk_v2.DataType_NAIVE_DATETIME, nil
	}

	if strings.HasPrefix(mysqlType, "year") {
		return fivetransdk_v2.DataType_INT, nil
	}

	if strings.HasPrefix(mysqlType, "varchar") ||
		strings.HasPrefix(mysqlType, "text") ||
		strings.HasPrefix(mysqlType, "enum") ||
		strings.HasPrefix(mysqlType, "char") {
		return fivetransdk_v2.DataType_STRING, nil
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
		return fivetransdk_v2.DataType_JSON, nil
	}

	switch mysqlType {
	case "date":
		return fivetransdk_v2.DataType_NAIVE_DATE, nil
	case "json":
		return fivetransdk_v2.DataType_JSON, nil
	case "tinytext":
		return fivetransdk_v2.DataType_STRING, nil
	case "mediumtext":
		return fivetransdk_v2.DataType_STRING, nil
	case "longtext":
		return fivetransdk_v2.DataType_STRING, nil
	case "binary":
		return fivetransdk_v2.DataType_BINARY, nil
	case "blob":
		return fivetransdk_v2.DataType_BINARY, nil
	case "longblob":
		return fivetransdk_v2.DataType_BINARY, nil
	case "mediumblob":
		return fivetransdk_v2.DataType_BINARY, nil
	case "tinyblob":
		return fivetransdk_v2.DataType_BINARY, nil
	case "bit":
		return fivetransdk_v2.DataType_BOOLEAN, nil
	case "time":
		return fivetransdk_v2.DataType_STRING, nil
	default:
		return fivetransdk_v2.DataType_STRING, nil
	}
}

// getDecimalParams parses the mysql type declaration for a column
// and returns a type compatible with fivetran's SDK
func getDecimalParams(mysqlType string) *fivetransdk_v2.DecimalParams {
	// From : https://dev.mysql.com/doc/refman/5.7/en/precision-math-decimal-characteristics.html
	// The declaration syntax for a DECIMAL column is DECIMAL(M,D). The ranges of values for the arguments are as follows:
	// M is the maximum number of digits (the precision). It has a range of 1 to 65.
	var m uint32 = 10
	// D is the number of digits to the right of the decimal point (the scale). It has a range of 0 to 30 and must be no larger than M.
	// If D is omitted, the default is 0. If M is omitted, the default is 10.
	var d uint32 = 0

	precision := strings.Replace(strings.ToUpper(mysqlType), "DECIMAL", "", 1)
	r := regexp.MustCompile(`[-]?\d[\d,]*[\d{2}]*`)
	matches := r.FindAllString(precision, -1)
	if len(matches) == 0 {
		fmt.Println("returning defaults")
		// return defaults
		return &fivetransdk_v2.DecimalParams{
			Precision: m,
			Scale:     d,
		}
	}

	parts := strings.Split(matches[0], ",")
	if len(parts) > 1 {
		di, err := strconv.ParseUint(parts[1], 10, 32)
		if err == nil && di <= math.MaxUint32 {
			d = uint32(di)
		}
	}

	mi, err := strconv.ParseUint(parts[0], 10, 32)
	if err == nil && mi <= math.MaxUint32 {
		m = uint32(mi)
	}
	return &fivetransdk_v2.DecimalParams{
		Precision: m,
		Scale:     d,
	}
}
