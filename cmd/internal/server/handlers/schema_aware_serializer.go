package handlers

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/planetscale/fivetran-source/lib"

	"github.com/pkg/errors"

	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var fivetranOpMap = map[lib.Operation]fivetransdk.RecordType{
	lib.OpType_Insert:   fivetransdk.RecordType_UPSERT,
	lib.OpType_Delete:   fivetransdk.RecordType_DELETE,
	lib.OpType_Update:   fivetransdk.RecordType_UPDATE,
	lib.OpType_Truncate: fivetransdk.RecordType_TRUNCATE,
}

type Serializer interface {
	Info(string) error
	Warning(string) error
	Severe(string) error
	Log(string, string) error
	Record(*sqltypes.Result, *fivetransdk.SchemaSelection, *fivetransdk.TableSelection, lib.Operation) error
	State(lib.SyncState) error
	Update(*lib.UpdatedRow, *fivetransdk.SchemaSelection, *fivetransdk.TableSelection) error
	Truncate(*fivetransdk.SchemaSelection, *fivetransdk.TableSelection) error
	SendWarningAlert(string) error
	SendTask(string) error
	RecordBinlogExpiration(keyspace, table, shard string)
	FlushBinlogExpirationWarning() error
}

// SourceColumnOrderer is implemented by a Serializer that knows the source
// schema read at the start of the sync. SourceColumnOrder returns every column
// of the table in table order, and false if the table is not in the schema.
// The sync handler uses it to detect column order changes (see
// lib.ColumnOrderChange); a Serializer without it skips that check.
type SourceColumnOrderer interface {
	SourceColumnOrder(keyspace, table string) ([]string, bool)
}

type LogSender interface {
	Send(*fivetransdk.UpdateResponse) error
}

// OutputStreamError reports that sending a response to Fivetran's output
// stream failed (for example the Update call was cancelled or its transport
// broke). Nothing more can reach Fivetran in this sync, so the sync handler
// ends the sync instead of streaming the remaining tables.
type OutputStreamError struct {
	Err error
}

func (e *OutputStreamError) Error() string {
	return fmt.Sprintf("sending to Fivetran failed: %v", e.Err)
}

func (e *OutputStreamError) Unwrap() error { return e.Err }

// GRPCStatus keeps the status of the failed Send.
func (e *OutputStreamError) GRPCStatus() *status.Status {
	return status.Convert(e.Err)
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
	binlogExpirations      binlogExpirationSet
}

type recordSerializer interface {
	Serialize(before *sqltypes.Result, after *sqltypes.Result, opType lib.Operation) ([]map[string]*fivetransdk.ValueType, error)
}

type schemaAwareRecordSerializer struct {
	columnSelection map[string]bool
	primaryKeys     map[string]bool
	columnWriters   map[string]func(value sqltypes.Value) (*fivetransdk.ValueType, error)
}

// RowSerializationError reports a source value that can't be converted to a
// Fivetran value. Error() names only the column and its type: neither the
// offending value nor the primary key values (possible PII) appear in it, so it
// is safe in Tasks, sync errors and logs. PrimaryKey is only written to the
// connector log by the sync handler (see serializationLogMessage).
type RowSerializationError struct {
	Column     string // column name
	ColumnType string // MySQL column type, e.g. "date" or "tinyint(1)"
	PrimaryKey string // e.g. "id=42" (sorted by column); connector log only, never in a Task
	Err        error  // converter error; may contain the value, never logged
}

func (e *RowSerializationError) Error() string {
	return fmt.Sprintf("unable to serialize row: a value in column %q (%s) can't be converted", e.Column, e.ColumnType)
}

func (e *RowSerializationError) Unwrap() error { return e.Err }

// GRPCStatus keeps the codes.Internal status this error had before it was typed.
func (e *RowSerializationError) GRPCStatus() *status.Status {
	return status.New(codes.Internal, e.Error())
}

func (s *schemaAwareRecordSerializer) newRowSerializationError(fields []*querypb.Field, row map[string]sqltypes.Value, colName string, err error) *RowSerializationError {
	columnType := ""
	for _, f := range fields {
		if f.Name == colName {
			columnType = f.ColumnType
			if columnType == "" {
				columnType = strings.ToLower(f.Type.String())
			}
			break
		}
	}
	pkColumns := make([]string, 0, len(s.primaryKeys))
	for pk := range s.primaryKeys {
		pkColumns = append(pkColumns, pk)
	}
	sort.Strings(pkColumns)
	pk := make([]string, 0, len(pkColumns))
	for _, c := range pkColumns {
		pk = append(pk, c+"="+row[c].ToString())
	}
	return &RowSerializationError{Column: colName, ColumnType: columnType, PrimaryKey: strings.Join(pk, ", "), Err: err}
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
		var err error
		beforeMap, err = convertRowToMap(&before.Rows[0], columns)
		if err != nil {
			return nil, err
		}
	}

	for _, row := range after.Rows {
		record := make(map[string]*fivetransdk.ValueType)
		var err error
		afterMap, err = convertRowToMap(&row, columns)
		if err != nil {
			return nil, err
		}
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
					return nil, s.newRowSerializationError(after.Fields, afterMap, colName, err)
				}
			}
			record[colName] = fVal
		}
		data = append(data, record)
	}

	return data, nil
}

func convertRowToMap(row *sqltypes.Row, columns []string) (map[string]sqltypes.Value, error) {
	if len(*row) != len(columns) {
		return nil, fmt.Errorf("row value count %d does not match column count %d", len(*row), len(columns))
	}

	record := map[string]sqltypes.Value{}
	for idx, val := range *row {
		colName := columns[idx]
		record[colName] = val
	}

	return record, nil
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

// SourceColumnOrder implements SourceColumnOrderer from the schema the
// serializer was built with (BuildSchema at the start of the sync, columns in
// ordinal_position order).
func (l *schemaAwareSerializer) SourceColumnOrder(keyspace, table string) ([]string, bool) {
	if l.schemaList == nil {
		return nil, false
	}
	for _, schema := range l.schemaList.Schemas {
		if schema.Name != keyspace {
			continue
		}
		for _, t := range schema.Tables {
			if t.Name != table {
				continue
			}
			if len(t.Columns) == 0 {
				return nil, false
			}
			columns := make([]string, 0, len(t.Columns))
			for _, c := range t.Columns {
				columns = append(columns, c.Name)
			}
			return columns, true
		}
	}
	return nil, false
}

func (l *schemaAwareSerializer) Info(msg string) error {
	return l.Log("INFO", msg)
}

func (l *schemaAwareSerializer) Warning(msg string) error {
	return l.Log("WARNING", msg)
}

func (l *schemaAwareSerializer) Severe(msg string) error {
	return l.Log("SEVERE", msg)
}

// send sends one response to Fivetran; a failure is an *OutputStreamError.
func (l *schemaAwareSerializer) send(response *fivetransdk.UpdateResponse) error {
	if err := l.sender.Send(response); err != nil {
		return &OutputStreamError{Err: err}
	}
	return nil
}

func (l *schemaAwareSerializer) SendWarningAlert(message string) error {
	warning := &fivetransdk.Warning{
		Message: message,
	}

	return l.send(&fivetransdk.UpdateResponse{
		Operation: &fivetransdk.UpdateResponse_Warning{
			Warning: warning,
		},
	})
}

// SendTask raises a Fivetran Task (dashboard Error) for the connection. Per the
// Partner SDK, only one Task may be sent per Update call and Fivetran stops the
// sync once it receives it, so callers must return right after sending.
// Fivetran resolves the Error after the next successful sync.
func (l *schemaAwareSerializer) SendTask(message string) error {
	return l.send(&fivetransdk.UpdateResponse{
		Operation: &fivetransdk.UpdateResponse_Task{
			Task: &fivetransdk.Task{
				Message: message,
			},
		},
	})
}

// RecordBinlogExpiration implements lib.BinlogExpirationRecorder.
func (l *schemaAwareSerializer) RecordBinlogExpiration(keyspace, table, shard string) {
	l.binlogExpirations.add(keyspace, table, shard)
}

// FlushBinlogExpirationWarning sends at most one combined Warning for all
// binlog expirations recorded since the last flush.
func (l *schemaAwareSerializer) FlushBinlogExpirationWarning() error {
	msg := l.binlogExpirations.warningMessage()
	l.binlogExpirations = binlogExpirationSet{}
	if msg == "" {
		return nil
	}
	return l.SendWarningAlert(msg)
}

func (l *schemaAwareSerializer) State(sc lib.SyncState) error {
	state, err := json.Marshal(sc)
	if err != nil {
		return l.Severe(fmt.Sprintf("marshal schema aware json serializer : %q", err))
	}
	return l.send(&fivetransdk.UpdateResponse{
		Operation: &fivetransdk.UpdateResponse_Checkpoint{
			Checkpoint: &fivetransdk.Checkpoint{
				StateJson: string(state),
			},
		},
	})
}

func (l *schemaAwareSerializer) Log(level string, msg string) error {
	log := map[string]interface{}{
		"level":          level,
		"message":        msg,
		"message-origin": "sdk_connector",
	}
	logJSON, _ := json.Marshal(log)
	fmt.Println(string(logJSON))
	return nil
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
		if err := l.Severe(msg); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		// Keep the typed error so the sync handler can raise a Task for it.
		var rse *RowSerializationError
		if errors.As(err, &rse) {
			return rse
		}
		return status.Error(codes.Internal, msg)
	}

	for _, row := range rows {
		updateResponse := l.recordResponse
		operationRecord, ok := updateResponse.Operation.(*fivetransdk.UpdateResponse_Record)
		if !ok {
			msg := fmt.Sprintf("recordResponse Operation is of type %T not UpdateResponse_Record", updateResponse.Operation)
			if err := l.Severe(msg); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			return status.Error(codes.Internal, msg)
		}
		operationRecord.Record.Data = row
		operationRecord.Record.Type = fivetranOpMap[opType]
		if err := l.send(l.recordResponse); err != nil {
			return err
		}
	}

	return nil
}

func (l *schemaAwareSerializer) sendTruncate(schema *fivetransdk.SchemaSelection, table *fivetransdk.TableSelection) error {
	return l.send(&fivetransdk.UpdateResponse{
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
