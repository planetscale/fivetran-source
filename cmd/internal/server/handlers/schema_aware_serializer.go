package handlers

import (
	"encoding/json"
	"fmt"

	"github.com/planetscale/fivetran-source/lib"

	"github.com/pkg/errors"

	fivetransdk "github.com/planetscale/fivetran-proto/go"
	"vitess.io/vitess/go/sqltypes"
)

type schemaAwareSerializer struct {
	prefix                 string
	sender                 LogSender
	recordResponseKey      string
	recordResponse         *fivetransdk.UpdateResponse
	serializeTinyIntAsBool bool
	serializers            map[string]*recordSerializer
	schema                 *fivetransdk.Schema
}

type recordSerializer interface {
	Serialize(result *sqltypes.Result) ([]map[string]*fivetransdk.ValueType, error)
}

type schemaAwareRecordSerializer struct {
	columnSelection map[string]bool
	columnWriters   map[string]func(value sqltypes.Value) (*fivetransdk.ValueType, error)
}

func (s *schemaAwareRecordSerializer) Serialize(result *sqltypes.Result) ([]map[string]*fivetransdk.ValueType, error) {
	data := make([]map[string]*fivetransdk.ValueType, 0, len(result.Rows))
	columns := make([]string, 0, len(result.Fields))
	for _, field := range result.Fields {
		columns = append(columns, field.Name)
	}

	for _, row := range result.Rows {
		record := make(map[string]*fivetransdk.ValueType)
		for idx, val := range row {
			if idx < len(columns) {

				colName := columns[idx]
				if selected := s.columnSelection[colName]; !selected {
					continue
				}
				var (
					fVal *fivetransdk.ValueType
					err  error
				)

				writer, ok := s.columnWriters[colName]
				if ok {
					fVal, err = writer(val)
				} else {
					return nil, fmt.Errorf("no column writer available for %v", colName)
				}

				if err != nil {
					return nil, errors.Wrap(err, "unable to serialize row")
				}
				record[colName] = fVal
			}
		}
		data = append(data, record)
	}

	return data, nil
}

func NewSchemaAwareSerializer(sender LogSender, prefix string, serializeTinyIntAsBool bool, schemaList *fivetransdk.SchemaList) Serializer {
	return &schemaAwareSerializer{
		prefix:                 prefix,
		sender:                 sender,
		serializeTinyIntAsBool: serializeTinyIntAsBool,
		schema:                 schemaList.Schemas[0],
	}
}

func (l *schemaAwareSerializer) Info(msg string) {
	l.Log(fivetransdk.LogLevel_INFO, msg)
}

func (l *schemaAwareSerializer) State(sc lib.SyncState) error {
	state, err := json.Marshal(sc)
	if err != nil {
		return l.Log(fivetransdk.LogLevel_SEVERE, fmt.Sprintf("%q", err))
	}
	return l.sender.Send(&fivetransdk.UpdateResponse{
		Response: &fivetransdk.UpdateResponse_Operation{
			Operation: &fivetransdk.Operation{
				Op: &fivetransdk.Operation_Checkpoint{
					Checkpoint: &fivetransdk.Checkpoint{
						StateJson: string(state),
					},
				},
			},
		},
	})
}

func (l *schemaAwareSerializer) Log(level fivetransdk.LogLevel, s string) error {
	return l.sender.Send(&fivetransdk.UpdateResponse{
		Response: &fivetransdk.UpdateResponse_LogEntry{
			LogEntry: &fivetransdk.LogEntry{
				Message: l.prefix + " " + s,
				Level:   level,
			},
		},
	})
}

func (l *schemaAwareSerializer) Release() {
	l.recordResponse = nil
	l.sender = nil
}

func (l *schemaAwareSerializer) Record(result *sqltypes.Result, schema *fivetransdk.SchemaSelection, table *fivetransdk.TableSelection) error {
	// make one response type per schema + table combination
	// so we can avoid instantiating one object per table, and instead
	// make one object per schema + table combo
	if responseKey(schema, table) != l.recordResponseKey {
		l.recordResponseKey = responseKey(schema, table)
		l.recordResponse = &fivetransdk.UpdateResponse{
			Response: &fivetransdk.UpdateResponse_Operation{
				Operation: &fivetransdk.Operation{
					Op: &fivetransdk.Operation_Record{
						Record: &fivetransdk.Record{
							SchemaName: &schema.SchemaName,
							TableName:  table.TableName,
							Type:       fivetransdk.OpType_UPSERT,
							Data:       nil,
						},
					},
				},
			},
		}
		if l.serializers == nil {
			l.serializers = map[string]*recordSerializer{}
		}

		if _, ok := l.serializers[l.recordResponseKey]; !ok {

			rs, err := generateRecordSerializer(table, l.schema, l.serializeTinyIntAsBool)
			if err != nil {
				return err
			}
			l.serializers[l.recordResponseKey] = &rs

		}
	}

	rs := *l.serializers[l.recordResponseKey]
	rows, err := rs.Serialize(result)
	if err != nil {
		return l.Log(fivetransdk.LogLevel_SEVERE, fmt.Sprintf("%q", err))
	}

	for _, row := range rows {
		operation, ok := l.recordResponse.Response.(*fivetransdk.UpdateResponse_Operation)
		if !ok {
			return l.Log(fivetransdk.LogLevel_SEVERE, "recordResponse Operation is not of type UpdateResponse_Operation")
		}
		operationRecord, ok := operation.Operation.Op.(*fivetransdk.Operation_Record)
		if !ok {
			return l.Log(fivetransdk.LogLevel_SEVERE, "recordResponse Operation.Op is not of type Operation_Record")
		}
		operationRecord.Record.Data = row
		l.sender.Send(l.recordResponse)
	}

	return nil
}

func generateRecordSerializer(table *fivetransdk.TableSelection, schema *fivetransdk.Schema, serializeTinyIntAsBool bool) (recordSerializer, error) {
	serializers := map[string]func(value sqltypes.Value) (*fivetransdk.ValueType, error){}
	var err error
	var tableSchema *fivetransdk.Table
	for _, tableWithSchema := range schema.Tables {
		if tableWithSchema.Name == table.TableName {
			tableSchema = tableWithSchema
		}
	}
	if tableSchema != nil {
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
				}
			}
		}
	}

	return &schemaAwareRecordSerializer{
		columnSelection: table.Columns,
		columnWriters:   serializers,
	}, nil
}
