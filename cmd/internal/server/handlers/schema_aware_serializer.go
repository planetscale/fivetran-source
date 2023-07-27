package handlers

import (
	"encoding/json"
	"fmt"

	"github.com/planetscale/fivetran-source/lib"

	"github.com/pkg/errors"

	fivetransdk_v2 "github.com/planetscale/fivetran-sdk-grpc/go"
	"vitess.io/vitess/go/sqltypes"
)

var fivetranOpMap = map[lib.Operation]fivetransdk_v2.OpType{
	lib.OpType_Insert: fivetransdk_v2.OpType_UPSERT,
	lib.OpType_Delete: fivetransdk_v2.OpType_DELETE,
	lib.OpType_Update: fivetransdk_v2.OpType_UPDATE,
}

type Serializer interface {
	Info(string)
	Log(fivetransdk_v2.LogLevel, string) error
	Record(*sqltypes.Result, *fivetransdk_v2.SchemaSelection, *fivetransdk_v2.TableSelection, lib.Operation) error
	State(lib.SyncState) error
	Update(*lib.UpdatedRow, *fivetransdk_v2.SchemaSelection, *fivetransdk_v2.TableSelection) error
}

type LogSender interface {
	Send(*fivetransdk_v2.UpdateResponse) error
}

type schemaAwareSerializer struct {
	prefix                 string
	sender                 LogSender
	recordResponseKey      string
	recordResponse         *fivetransdk_v2.UpdateResponse
	serializeTinyIntAsBool bool
	serializers            map[string]*recordSerializer
	schemaList             *fivetransdk_v2.SchemaList
}

type recordSerializer interface {
	Serialize(before *sqltypes.Result, after *sqltypes.Result, opType lib.Operation) ([]map[string]*fivetransdk_v2.ValueType, error)
}

type schemaAwareRecordSerializer struct {
	columnSelection map[string]bool
	primaryKeys     map[string]bool
	columnWriters   map[string]func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error)
}

func (s *schemaAwareRecordSerializer) Serialize(before *sqltypes.Result, after *sqltypes.Result, opType lib.Operation) ([]map[string]*fivetransdk_v2.ValueType, error) {
	data := make([]map[string]*fivetransdk_v2.ValueType, 0, len(after.Rows))
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
		record := make(map[string]*fivetransdk_v2.ValueType)
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

			fVal, err := writer(val)
			if err != nil {
				return nil, errors.Wrap(err, "unable to serialize row")
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

func NewSchemaAwareSerializer(sender LogSender, prefix string, serializeTinyIntAsBool bool, schemaList *fivetransdk_v2.SchemaList) Serializer {
	return &schemaAwareSerializer{
		prefix:                 prefix,
		sender:                 sender,
		serializeTinyIntAsBool: serializeTinyIntAsBool,
		schemaList:             schemaList,
		serializers:            map[string]*recordSerializer{},
	}
}

func (l *schemaAwareSerializer) Info(msg string) {
	l.Log(fivetransdk_v2.LogLevel_INFO, msg)
}

func (l *schemaAwareSerializer) State(sc lib.SyncState) error {
	state, err := json.Marshal(sc)
	if err != nil {
		return l.Log(fivetransdk_v2.LogLevel_SEVERE, fmt.Sprintf("marshal schema aware json serializer : %q", err))
	}
	return l.sender.Send(&fivetransdk_v2.UpdateResponse{
		Response: &fivetransdk_v2.UpdateResponse_Operation{
			Operation: &fivetransdk_v2.Operation{
				Op: &fivetransdk_v2.Operation_Checkpoint{
					Checkpoint: &fivetransdk_v2.Checkpoint{
						StateJson: string(state),
					},
				},
			},
		},
	})
}

func (l *schemaAwareSerializer) Log(level fivetransdk_v2.LogLevel, s string) error {
	return l.sender.Send(&fivetransdk_v2.UpdateResponse{
		Response: &fivetransdk_v2.UpdateResponse_LogEntry{
			LogEntry: &fivetransdk_v2.LogEntry{
				Message: l.prefix + " " + s,
				Level:   level,
			},
		},
	})
}

// Update is responsible for creating a record that has the following values :
// 1. Primary keys of the row that was updated.
// 2. All changed values between the Before & After fields.
func (l *schemaAwareSerializer) Update(updatedRow *lib.UpdatedRow, schema *fivetransdk_v2.SchemaSelection, table *fivetransdk_v2.TableSelection) error {
	return l.serializeResult(updatedRow.Before, updatedRow.After, schema, table, lib.OpType_Update)
}

func (l *schemaAwareSerializer) Record(result *sqltypes.Result, schema *fivetransdk_v2.SchemaSelection, table *fivetransdk_v2.TableSelection, opType lib.Operation) error {
	return l.serializeResult(nil, result, schema, table, opType)
}

func (l *schemaAwareSerializer) serializeResult(before *sqltypes.Result, after *sqltypes.Result, schema *fivetransdk_v2.SchemaSelection, table *fivetransdk_v2.TableSelection, opType lib.Operation) error {
	// make one response type per schema + table combination
	// so we can avoid instantiating one object per table, and instead
	// make one object per schema + table combo
	if responseKey(schema, table) != l.recordResponseKey {
		l.recordResponseKey = responseKey(schema, table)
		l.recordResponse = &fivetransdk_v2.UpdateResponse{
			Response: &fivetransdk_v2.UpdateResponse_Operation{
				Operation: &fivetransdk_v2.Operation{
					Op: &fivetransdk_v2.Operation_Record{
						Record: &fivetransdk_v2.Record{
							SchemaName: &schema.SchemaName,
							TableName:  table.TableName,
							Type:       fivetransdk_v2.OpType_UPSERT,
							Data:       nil,
						},
					},
				},
			},
		}

		if _, ok := l.serializers[l.recordResponseKey]; !ok {

			rs, err := generateRecordSerializer(table, schema.SchemaName, l.schemaList, l.serializeTinyIntAsBool)
			if err != nil {
				return err
			}
			l.serializers[l.recordResponseKey] = &rs

		}
	}

	rs := *l.serializers[l.recordResponseKey]
	rows, err := rs.Serialize(before, after, opType)
	if err != nil {
		return l.Log(fivetransdk_v2.LogLevel_SEVERE, fmt.Sprintf("record schema aware json serializer : %q", err))
	}

	for _, row := range rows {
		operation, ok := l.recordResponse.Response.(*fivetransdk_v2.UpdateResponse_Operation)
		if !ok {
			return l.Log(fivetransdk_v2.LogLevel_SEVERE, fmt.Sprintf("recordResponse Operation is of type %T, not UpdateResponse_Operation", l.recordResponse.Response))
		}
		operationRecord, ok := operation.Operation.Op.(*fivetransdk_v2.Operation_Record)
		if !ok {
			return l.Log(fivetransdk_v2.LogLevel_SEVERE, fmt.Sprintf("recordResponse Operation.Op is of type %T not Operation_Record", operation.Operation.Op))
		}
		operationRecord.Record.Data = row
		operationRecord.Record.Type = fivetranOpMap[opType]
		l.sender.Send(l.recordResponse)
	}

	return nil
}

func generateRecordSerializer(table *fivetransdk_v2.TableSelection, selectedSchemaName string, schemaList *fivetransdk_v2.SchemaList, serializeTinyIntAsBool bool) (recordSerializer, error) {
	serializers := map[string]func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error){}
	var err error
	pks := map[string]bool{}
	for _, schema := range schemaList.Schemas {
		if schema.Name != selectedSchemaName {
			continue
		}

		var tableSchema *fivetransdk_v2.Table
		for _, tableWithSchema := range schema.Tables {
			if tableWithSchema.Name == table.TableName {
				tableSchema = tableWithSchema
			}
		}

		if tableSchema == nil {
			return nil, fmt.Errorf("cannot generate serializer, unable to find schema for table : %q", table.TableName)
		}

		for colName, included := range table.Columns {
			if !included {
				continue
			}

			for _, colunWithSchema := range tableSchema.Columns {
				if colName == colunWithSchema.Name {
					serializers[colName], err = GetConverter(colunWithSchema.Type, serializeTinyIntAsBool)
					if err != nil {
						return nil, err
					}

					if colunWithSchema.PrimaryKey {
						pks[colunWithSchema.Name] = true
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

func responseKey(schema *fivetransdk_v2.SchemaSelection, table *fivetransdk_v2.TableSelection) string {
	return schema.SchemaName + ":" + table.TableName
}
