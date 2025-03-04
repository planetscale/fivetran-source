package handlers

import (
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/planetscale/fivetran-source/lib"

	"github.com/pkg/errors"

	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk"
	"vitess.io/vitess/go/sqltypes"
)

// var fivetranOpMap = map[lib.Operation]fivetransdk.OpType{
// 	lib.OpType_Insert:   fivetransdk.OpType_UPSERT,
// 	lib.OpType_Delete:   fivetransdk.OpType_DELETE,
// 	lib.OpType_Update:   fivetransdk.OpType_UPDATE,
// 	lib.OpType_Truncate: fivetransdk.OpType_TRUNCATE,
// }

var fivetranOpMap = map[lib.Operation]fivetransdk.RecordType{
	lib.OpType_Insert:   fivetransdk.RecordType_UPSERT,
	lib.OpType_Delete:   fivetransdk.RecordType_DELETE,
	lib.OpType_Update:   fivetransdk.RecordType_UPDATE,
	lib.OpType_Truncate: fivetransdk.RecordType_TRUNCATE,
}

type Serializer interface {
	Info(string) error
	Warning(string) error
	Log(*fivetransdk.UpdateResponse) error
	Record(*sqltypes.Result, *fivetransdk.SchemaSelection, *fivetransdk.TableSelection, lib.Operation) error
	State(lib.SyncState) error
	Update(*lib.UpdatedRow, *fivetransdk.SchemaSelection, *fivetransdk.TableSelection) error
	Truncate(*fivetransdk.SchemaSelection, *fivetransdk.TableSelection) error
}

type LogSender interface {
	Send(*fivetransdk.UpdateResponse) error
}

type schemaAwareSerializer struct {
	prefix                 string
	sender                 LogSender
	recordResponseKey      string
	recordResponse         *fivetransdk.UpdateResponse
	serializeTinyIntAsBool bool
	serializers            map[string]*recordSerializer
	schemaList             *fivetransdk.SchemaList
	enumsAndSets           SchemaEnumsAndSets
}

type recordSerializer interface {
	Serialize(before *sqltypes.Result, after *sqltypes.Result, opType lib.Operation) ([]map[string]*fivetransdk.ValueType, error)
}

type schemaAwareRecordSerializer struct {
	columnSelection map[string]bool
	primaryKeys     map[string]bool
	columnWriters   map[string]func(value sqltypes.Value) (*fivetransdk.ValueType, error)
}

func (s *schemaAwareRecordSerializer) Serialize(before *sqltypes.Result, after *sqltypes.Result, opType lib.Operation) ([]map[string]*fivetransdk.ValueType, error) {
	data := make([]map[string]*fivetransdk.ValueType, 0, len(after.Rows))
	columns := make([]string, 0, len(after.Fields))
	for _, field := range after.Fields {
		columns = append(columns, field.Name)
	}

	if opType == lib.OpType_Update && !(len(before.Rows) == 1 && len(after.Rows) == 1) {
		return nil, fmt.Errorf("unable to serialize update, found [%v] rows in before, [%v] in after", len(before.Rows), len(after.Rows))
	}

	var (
		afterMap  map[string]sqltypes.Value
		beforeMap map[string]sqltypes.Value
	)

	if opType == lib.OpType_Update {
		beforeMap = convertRowToMap(&before.Rows[0], columns)
	}

	for _, row := range after.Rows {
		record := make(map[string]*fivetransdk.ValueType)
		afterMap = convertRowToMap(&row, columns)
		for colName, val := range afterMap {
			if selected := s.columnSelection[colName]; !selected {
				continue
			}

			writer, ok := s.columnWriters[colName]
			if !ok {
				return nil, fmt.Errorf("no column writer available for %v", colName)
			}

			// Write out all columns for insert operations.
			writeColumn := opType == lib.OpType_Insert

			if !writeColumn {
				// Write primary keys for delete & update operations
				writeColumn = s.primaryKeys[colName]
			}

			if !writeColumn && opType == lib.OpType_Update && beforeMap != nil {
				// Only write changed values for update operations
				writeColumn = beforeMap[colName].String() != afterMap[colName].String()
			}

			if !writeColumn {
				continue
			}
			var (
				fVal *fivetransdk.ValueType
				err  error
			)
			if val.IsNull() {
				fVal = &fivetransdk.ValueType{
					Inner: &fivetransdk.ValueType_Null{Null: true},
				}
			} else {
				fVal, err = writer(val)
				if err != nil {
					return nil, errors.Wrap(err, "unable to serialize row")
				}
			}
			record[colName] = fVal
		}
		data = append(data, record)
	}

	return data, nil
}

func convertRowToMap(row *sqltypes.Row, columns []string) map[string]sqltypes.Value {
	record := map[string]sqltypes.Value{}
	for idx, val := range *row {
		if idx > len(columns) {
			// if there's more values than columns, exit this loop
			break
		}
		colName := columns[idx]
		record[colName] = val
	}

	return record
}

func NewSchemaAwareSerializer(sender LogSender, prefix string, serializeTinyIntAsBool bool, schemaList *fivetransdk.SchemaList, enumsAndSets SchemaEnumsAndSets) Serializer {
	return &schemaAwareSerializer{
		prefix:                 prefix,
		sender:                 sender,
		serializeTinyIntAsBool: serializeTinyIntAsBool,
		schemaList:             schemaList,
		enumsAndSets:           enumsAndSets,
		serializers:            map[string]*recordSerializer{},
	}
}

func (l *schemaAwareSerializer) Info(msg string) error {
	return l.Log(&fivetransdk.UpdateResponse{
		Operation: &fivetransdk.UpdateResponse_Task{
			Task: &fivetransdk.Task{
				Message: msg,
			},
		},
	})
}

func (l *schemaAwareSerializer) Warning(msg string) error {
	return l.Log(&fivetransdk.UpdateResponse{
		Operation: &fivetransdk.UpdateResponse_Warning{
			Warning: &fivetransdk.Warning{
				Message: msg,
			},
		},
	})
}

func (l *schemaAwareSerializer) State(sc lib.SyncState) error {
	state, err := json.Marshal(sc)
	if err != nil {
		return l.Warning(fmt.Sprintf("marshal schema aware json serializer : %q", err))
	}
	return l.sender.Send(&fivetransdk.UpdateResponse{
		Operation: &fivetransdk.UpdateResponse_Checkpoint{
			Checkpoint: &fivetransdk.Checkpoint{
				StateJson: string(state),
			},
		},
	})
}

func (l *schemaAwareSerializer) Log(response *fivetransdk.UpdateResponse) error {
	return l.sender.Send(response)
}

// Update is responsible for creating a record that has the following values :
// 1. Primary keys of the row that was updated.
// 2. All changed values between the Before & After fields.
func (l *schemaAwareSerializer) Update(updatedRow *lib.UpdatedRow, schema *fivetransdk.SchemaSelection, table *fivetransdk.TableSelection) error {
	return l.serializeResult(updatedRow.Before, updatedRow.After, schema, table, lib.OpType_Update)
}

func (l *schemaAwareSerializer) Truncate(schema *fivetransdk.SchemaSelection, table *fivetransdk.TableSelection) error {
	return l.sendTruncate(schema, table)
}

func (l *schemaAwareSerializer) Record(result *sqltypes.Result, schema *fivetransdk.SchemaSelection, table *fivetransdk.TableSelection, opType lib.Operation) error {
	return l.serializeResult(nil, result, schema, table, opType)
}

func (l *schemaAwareSerializer) serializeResult(before *sqltypes.Result, after *sqltypes.Result, schema *fivetransdk.SchemaSelection, table *fivetransdk.TableSelection, opType lib.Operation) error {
	// make one response type per schema + table combination
	// so we can avoid instantiating one object per table, and instead
	// make one object per schema + table combo
	if responseKey(schema, table) != l.recordResponseKey {
		l.recordResponseKey = responseKey(schema, table)
		l.recordResponse = &fivetransdk.UpdateResponse{
			Operation: &fivetransdk.UpdateResponse_Record{
				Record: &fivetransdk.Record{
					SchemaName: &schema.SchemaName,
					TableName:  table.TableName,
					Type:       fivetransdk.RecordType_UPSERT,
					Data:       nil,
				},
			},
		}

		if _, ok := l.serializers[l.recordResponseKey]; !ok {
			rs, err := l.generateRecordSerializer(table, schema.SchemaName)
			if err != nil {
				return err
			}
			l.serializers[l.recordResponseKey] = &rs
		}
	}

	rs := *l.serializers[l.recordResponseKey]
	rows, err := rs.Serialize(before, after, opType)
	if err != nil {
		msg := fmt.Sprintf("record schema aware json serializer : %q", err)
		if err := l.Warning(msg); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		return status.Error(codes.Internal, msg)
	}

	for _, row := range rows {
		updateResponse := l.recordResponse
		operationRecord, ok := updateResponse.Operation.(*fivetransdk.UpdateResponse_Record)
		if !ok {
			msg := fmt.Sprintf("recordResponse Operation is of type %T not UpdateResponse_Record", updateResponse.Operation)
			if err := l.Warning(msg); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			return status.Error(codes.Internal, msg)
		}
		operationRecord.Record.Data = row
		operationRecord.Record.Type = fivetranOpMap[opType]
		l.sender.Send(l.recordResponse)
	}

	return nil
}

func (l *schemaAwareSerializer) sendTruncate(schema *fivetransdk.SchemaSelection, table *fivetransdk.TableSelection) error {
	return l.sender.Send(&fivetransdk.UpdateResponse{
		Operation: &fivetransdk.UpdateResponse_Record{
			Record: &fivetransdk.Record{
				SchemaName: &schema.SchemaName,
				TableName:  table.TableName,
				Type:       fivetransdk.RecordType_TRUNCATE,
				Data:       nil,
			},
		},
	})
}

func (l *schemaAwareSerializer) generateRecordSerializer(table *fivetransdk.TableSelection, selectedSchemaName string) (recordSerializer, error) {
	serializers := map[string]func(value sqltypes.Value) (*fivetransdk.ValueType, error){}
	var err error
	pks := map[string]bool{}
	for _, schema := range l.schemaList.Schemas {
		if schema.Name != selectedSchemaName {
			continue
		}

		schemaEnumAndSetValues, ok := l.enumsAndSets[schema.Name]
		if !ok {
			schemaEnumAndSetValues = map[string]map[string]ValueMap{}
		}

		var tableSchema *fivetransdk.Table
		for _, tableWithSchema := range schema.Tables {
			if tableWithSchema.Name == table.TableName {
				tableSchema = tableWithSchema
			}
		}

		if tableSchema == nil {
			return nil, fmt.Errorf("cannot generate serializer, unable to find schema for table : %q", table.TableName)
		}

		tableSchemaEnumAndSetValues, ok := schemaEnumAndSetValues[tableSchema.Name]
		if !ok {
			tableSchemaEnumAndSetValues = map[string]ValueMap{}
		}

		for colName, included := range table.Columns {
			if !included {
				continue
			}

			for _, columnWithSchema := range tableSchema.Columns {
				if colName == columnWithSchema.Name {
					if tableSchemaEnumAndSetValues[colName].columnType == "enum" {
						values := tableSchemaEnumAndSetValues[colName].values
						msg := fmt.Sprintf("using enum converter for column %s and values %+v", colName, values)
						l.Info(msg)
						serializers[colName], err = GetEnumConverter(values)
						if err != nil {
							return nil, err
						}
					} else if tableSchemaEnumAndSetValues[colName].columnType == "set" {
						values := tableSchemaEnumAndSetValues[colName].values
						msg := fmt.Sprintf("using set converter for column %s and values %+v", colName, values)
						l.Info(msg)
						serializers[colName], err = GetSetConverter(values)
						if err != nil {
							return nil, err
						}
					} else {
						serializers[colName], err = GetConverter(columnWithSchema.Type)
						if err != nil {
							return nil, err
						}
					}

					if columnWithSchema.PrimaryKey {
						pks[columnWithSchema.Name] = true
					}
				}
			}
		}

	}

	return &schemaAwareRecordSerializer{
		columnSelection: table.Columns,
		primaryKeys:     pks,
		columnWriters:   serializers,
	}, nil
}

func responseKey(schema *fivetransdk.SchemaSelection, table *fivetransdk.TableSelection) string {
	return schema.SchemaName + ":" + table.TableName
}
