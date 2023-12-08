package handlers

import (
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/planetscale/fivetran-source/lib"

	fivetransdk "github.com/planetscale/fivetran-sdk-grpc/go"
)

const (
	gCTableNameExpression string = `^_vt_(HOLD|PURGE|EVAC|DROP)_([0-f]{32})_([0-9]{14})$`
)

// Maps enum and set indices to their values
type ValueMap struct {
	columnType string
	values     []string
}

type SchemaEnumsAndSets map[string]map[string]map[string]ValueMap

type SchemaWithMetadata struct {
	SchemaList   *fivetransdk.SchemaList
	EnumsAndSets SchemaEnumsAndSets
}

var gcTableNameRegexp = regexp.MustCompile(gCTableNameExpression)

type FiveTranSchemaBuilder struct {
	schemas               map[string]*fivetransdk.Schema
	tables                map[string]map[string]*fivetransdk.Table
	treatTinyIntAsBoolean bool
	// We need to map enum and set indices to their actual values
	// so that we can properly serialize them in the Update phase.
	// This looks like keyspace -> table -> column -> enum/set values
	enumsAndSets SchemaEnumsAndSets
}

func NewSchemaBuilder(treatTinyIntAsBoolean bool) lib.SchemaBuilder {
	return &FiveTranSchemaBuilder{
		treatTinyIntAsBoolean: treatTinyIntAsBoolean,
		enumsAndSets:          map[string]map[string]map[string]ValueMap{},
	}
}

func (s *FiveTranSchemaBuilder) OnKeyspace(keyspaceName string) {
	schema := &fivetransdk.Schema{
		Name:   keyspaceName,
		Tables: []*fivetransdk.Table{},
	}
	if s.schemas == nil {
		s.schemas = map[string]*fivetransdk.Schema{}
	}
	if s.enumsAndSets[keyspaceName] == nil {
		s.enumsAndSets[keyspaceName] = map[string]map[string]ValueMap{}
	}

	s.schemas[keyspaceName] = schema
}

func (s *FiveTranSchemaBuilder) OnTable(keyspaceName, tableName string) {
	// skip any that are Vitess's GC tables.
	if gcTableNameRegexp.MatchString(tableName) {
		return
	}

	s.getOrCreateTable(keyspaceName, tableName)
}

func (s *FiveTranSchemaBuilder) getOrCreateTable(keyspaceName string, tableName string) *fivetransdk.Table {
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

	// setting up for enum and set value mappings
	keyspace, ok := s.enumsAndSets[keyspaceName]
	if !ok {
		s.enumsAndSets[keyspaceName] = map[string]map[string]ValueMap{}
	}
	if keyspace[tableName] == nil {
		keyspace[tableName] = map[string]ValueMap{}
	}

	return table
}

func (s *FiveTranSchemaBuilder) OnColumns(keyspaceName, tableName string, columns []lib.MysqlColumn) {
	table := s.getOrCreateTable(keyspaceName, tableName)
	if table.Columns == nil {
		table.Columns = []*fivetransdk.Column{}
	}

	if s.enumsAndSets[keyspaceName] == nil {
		s.enumsAndSets[keyspaceName] = map[string]map[string]ValueMap{}
	}
	if s.enumsAndSets[keyspaceName][tableName] == nil {
		s.enumsAndSets[keyspaceName][tableName] = map[string]ValueMap{}
	}
	enumAndSetValues := s.enumsAndSets[keyspaceName][tableName]

	for _, column := range columns {
		dataType, decimalParams := getFivetranDataType(column.Type, s.treatTinyIntAsBoolean)
		table.Columns = append(table.Columns, &fivetransdk.Column{
			Name:       column.Name,
			Type:       dataType,
			Decimal:    decimalParams,
			PrimaryKey: column.IsPrimaryKey,
		})

		if isEnumOrSet(column.Type) {
			enumAndSetValues[column.Name] = parseEnumOrSetValues(column.Type)
		}
	}
}

func (s *FiveTranSchemaBuilder) BuildResponse() (*fivetransdk.SchemaResponse, error) {
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

// BuildUpdateResponse returns a schema with extra metadata needed during the Update phase.
// This includes enum and set mappings not available on the fivetransdk Column type.
func (s *FiveTranSchemaBuilder) BuildUpdateResponse() (*SchemaWithMetadata, error) {
	responseSchema := &fivetransdk.SchemaResponse_WithSchema{
		WithSchema: &fivetransdk.SchemaList{},
	}

	for _, schema := range s.schemas {
		responseSchema.WithSchema.Schemas = append(responseSchema.WithSchema.Schemas, schema)
	}

	schema := fivetransdk.SchemaResponse{
		Response: responseSchema,
	}

	resp := &SchemaWithMetadata{
		SchemaList:   schema.GetWithSchema(),
		EnumsAndSets: s.enumsAndSets,
	}

	return resp, nil
}

func isEnumOrSet(mType string) bool {
	mysqlType := strings.ToLower(mType)
	return strings.HasPrefix(mysqlType, "enum") || strings.HasPrefix(mysqlType, "set")
}

// Takes enum or set column type like ENUM('a','b','c') or SET('a','b','c')
// and returns a slice of values []string{'a', 'b', 'c'}
func parseEnumOrSetValues(mType string) ValueMap {
	values := []string{}
	var columnType string
	if strings.HasPrefix(mType, "enum") {
		columnType = "enum"
	} else if strings.HasPrefix(mType, "set") {
		columnType = "set"
	}

	re := regexp.MustCompile(`\((.+)\)`)
	res := re.FindString(mType)
	res = strings.Trim(res, "(")
	res = strings.Trim(res, ")")
	for _, r := range strings.Split(res, ",") {
		values = append(values, strings.Trim(r, "'"))
	}

	return ValueMap{
		columnType: columnType,
		values:     values,
	}
}

// Convert columnType to fivetran type
func getFivetranDataType(mType string, treatTinyIntAsBoolean bool) (fivetransdk.DataType, *fivetransdk.DecimalParams) {
	mysqlType := strings.ToLower(mType)
	if strings.HasPrefix(mysqlType, "tinyint") {
		if treatTinyIntAsBoolean && mysqlType == "tinyint(1)" {
			return fivetransdk.DataType_BOOLEAN, nil
		}

		return fivetransdk.DataType_INT, nil
	}

	// A BIT(n) type can have a length of 1 to 64
	// we serialize these as a LONG : int64 value when serializing rows.
	if strings.HasPrefix(mysqlType, "bit") {
		return fivetransdk.DataType_LONG, nil
	}

	if strings.HasPrefix(mysqlType, "varbinary") {
		return fivetransdk.DataType_BINARY, nil
	}

	if strings.HasPrefix(mysqlType, "binary") {
		return fivetransdk.DataType_BINARY, nil
	}

	if strings.HasPrefix(mysqlType, "int") {
		if strings.Contains(mysqlType, "unsigned") {
			return fivetransdk.DataType_LONG, nil
		}
		return fivetransdk.DataType_INT, nil
	}

	if strings.HasPrefix(mysqlType, "smallint") {
		return fivetransdk.DataType_INT, nil
	}

	if strings.HasPrefix(mysqlType, "bigint") {
		return fivetransdk.DataType_LONG, nil
	}

	if strings.HasPrefix(mysqlType, "decimal") {
		return fivetransdk.DataType_DECIMAL, getDecimalParams(mysqlType)
	}

	if strings.HasPrefix(mysqlType, "double") ||
		strings.HasPrefix(mysqlType, "float") {
		return fivetransdk.DataType_DOUBLE, nil
	}

	if strings.HasPrefix(mysqlType, "timestamp") {
		return fivetransdk.DataType_UTC_DATETIME, nil
	}

	if strings.HasPrefix(mysqlType, "time") {
		return fivetransdk.DataType_STRING, nil
	}

	if strings.HasPrefix(mysqlType, "datetime") {
		return fivetransdk.DataType_NAIVE_DATETIME, nil
	}

	if strings.HasPrefix(mysqlType, "year") {
		return fivetransdk.DataType_INT, nil
	}

	if strings.HasPrefix(mysqlType, "varchar") ||
		strings.HasPrefix(mysqlType, "text") ||
		strings.HasPrefix(mysqlType, "enum") ||
		strings.HasPrefix(mysqlType, "char") {
		return fivetransdk.DataType_STRING, nil
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
		return fivetransdk.DataType_JSON, nil
	}

	switch mysqlType {
	case "date":
		return fivetransdk.DataType_NAIVE_DATE, nil
	case "json":
		return fivetransdk.DataType_JSON, nil
	case "tinytext":
		return fivetransdk.DataType_STRING, nil
	case "mediumtext":
		return fivetransdk.DataType_STRING, nil
	case "mediumint":
		return fivetransdk.DataType_INT, nil
	case "longtext":
		return fivetransdk.DataType_STRING, nil
	case "binary":
		return fivetransdk.DataType_BINARY, nil
	case "blob":
		return fivetransdk.DataType_BINARY, nil
	case "longblob":
		return fivetransdk.DataType_BINARY, nil
	case "mediumblob":
		return fivetransdk.DataType_BINARY, nil
	case "tinyblob":
		return fivetransdk.DataType_BINARY, nil
	case "bit":
		return fivetransdk.DataType_BOOLEAN, nil
	case "time":
		return fivetransdk.DataType_STRING, nil
	default:
		return fivetransdk.DataType_STRING, nil
	}
}

// getDecimalParams parses the mysql type declaration for a column
// and returns a type compatible with fivetran's SDK
func getDecimalParams(mysqlType string) *fivetransdk.DecimalParams {
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
		// return defaults
		return &fivetransdk.DecimalParams{
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
	return &fivetransdk.DecimalParams{
		Precision: m,
		Scale:     d,
	}
}
