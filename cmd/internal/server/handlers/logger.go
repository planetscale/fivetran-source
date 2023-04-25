package handlers

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/planetscale/fivetran-source/lib"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pkg/errors"

	fivetransdk "github.com/planetscale/fivetran-proto/go"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var fivetranOpMap = map[lib.Operation]fivetransdk.OpType{
	lib.OpType_Insert: fivetransdk.OpType_UPSERT,
	lib.OpType_Delete: fivetransdk.OpType_DELETE,
	lib.OpType_Update: fivetransdk.OpType_UPDATE,
}

type Logger interface {
	Info(string)
	Log(fivetransdk.LogLevel, string) error
	Record(*sqltypes.Result, *fivetransdk.SchemaSelection, *fivetransdk.TableSelection, lib.Operation) error
	State(lib.SyncState) error
	Release()
}

type LogSender interface {
	Send(*fivetransdk.UpdateResponse) error
}

type logger struct {
	prefix                 string
	sender                 LogSender
	recordResponseKey      string
	recordResponse         *fivetransdk.UpdateResponse
	serializeTinyIntAsBool bool
}

func NewLogger(sender LogSender, prefix string, serializeTinyIntAsBool bool) Logger {
	return &logger{
		prefix:                 prefix,
		sender:                 sender,
		serializeTinyIntAsBool: serializeTinyIntAsBool,
	}
}

func (l *logger) Info(msg string) {
	l.Log(fivetransdk.LogLevel_INFO, msg)
}

func (l *logger) State(sc lib.SyncState) error {
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

func (l *logger) Log(level fivetransdk.LogLevel, s string) error {
	return l.sender.Send(&fivetransdk.UpdateResponse{
		Response: &fivetransdk.UpdateResponse_LogEntry{
			LogEntry: &fivetransdk.LogEntry{
				Message: l.prefix + " " + s,
				Level:   level,
			},
		},
	})
}

func (l *logger) Release() {
	l.recordResponse = nil
	l.sender = nil
}

func (l *logger) Record(result *sqltypes.Result, schema *fivetransdk.SchemaSelection, table *fivetransdk.TableSelection, opType lib.Operation) error {
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
	}

	rows, err := queryResultToData(result, l.serializeTinyIntAsBool, table)
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
		operationRecord.Record.Type = fivetranOpMap[opType]
		l.sender.Send(l.recordResponse)
	}

	return nil
}

func responseKey(schema *fivetransdk.SchemaSelection, table *fivetransdk.TableSelection) string {
	return schema.SchemaName + ":" + table.TableName
}

func queryResultToData(result *sqltypes.Result, serializeTinyIntAsBool bool, table *fivetransdk.TableSelection) ([]map[string]*fivetransdk.ValueType, error) {
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
				if selected := table.Columns[colName]; !selected {
					continue
				}

				val, err := sqlTypeToValueType(val, serializeTinyIntAsBool)
				if err != nil {
					return nil, errors.Wrap(err, "unable to serialize row")
				}
				record[colName] = val
			}
		}
		data = append(data, record)
	}

	return data, nil
}

func sqlTypeToValueType(value sqltypes.Value, serializeTinyIntAsBool bool) (*fivetransdk.ValueType, error) {
	if value.IsNull() {
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Null{Null: true},
		}, nil
	}

	switch value.Type() {
	case querypb.Type_VARCHAR, querypb.Type_TEXT:
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_String_{String_: value.ToString()},
		}, nil
	case querypb.Type_INT8:
		i, err := value.ToInt64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Type_INT8")
		}

		if serializeTinyIntAsBool && i <= 1 {
			b, err := value.ToBool()
			if err != nil {
				return nil, errors.Wrap(err, "failed to serialize Type_INT8")
			}
			return &fivetransdk.ValueType{
				Inner: &fivetransdk.ValueType_Bool{Bool: b},
			}, nil
		}

		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Short{Short: int32(i)},
		}, nil

	case querypb.Type_DECIMAL:
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Decimal{Decimal: value.ToString()},
		}, nil

	case querypb.Type_BINARY, querypb.Type_VARBINARY, querypb.Type_BLOB:
		b, err := value.ToBytes()
		if err != nil {
			return nil, err
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Binary{Binary: b},
		}, nil
	case querypb.Type_JSON:
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Json{Json: value.ToString()},
		}, nil

	case querypb.Type_INT16, querypb.Type_UINT8, querypb.Type_INT32, querypb.Type_INT24, querypb.Type_UINT16, querypb.Type_UINT24:
		i, err := value.ToInt64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Type_INT32")
		}
		if i > math.MaxInt32 {
			return nil, errors.Wrap(err, "Int32 value will overflow")
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Short{Short: int32(i)},
		}, nil

	case querypb.Type_INT64, querypb.Type_UINT32, querypb.Type_UINT64:
		i, err := value.ToInt64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Type_INT64")
		}

		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Long{Long: i},
		}, nil

	case querypb.Type_FLOAT64:
		f, err := value.ToFloat64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Type_FLOAT64")
		}

		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Double{Double: f},
		}, nil

	case querypb.Type_FLOAT32:
		f, err := value.ToFloat64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Type_FLOAT32")
		}
		if f > math.MaxFloat32 {
			return nil, errors.Wrap(err, "Float32 value will overflow")
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Float{Float: float32(f)},
		}, nil
	case querypb.Type_DATE:
		t, err := time.Parse("2006-01-02", value.ToString())
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Type_DATE")
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_NaiveDate{NaiveDate: timestamppb.New(t)},
		}, nil
	case querypb.Type_TIMESTAMP:
		t, err := time.Parse("2006-01-02 15:04:05", value.ToString())
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Type_TIMESTAMP")
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_UtcDatetime{UtcDatetime: timestamppb.New(t)},
		}, nil
	case querypb.Type_DATETIME:
		t, err := time.Parse("2006-01-02 15:04:05", value.ToString())
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Type_DATETIME")
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_NaiveDatetime{NaiveDatetime: timestamppb.New(t)},
		}, nil
	case querypb.Type_TIME:
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_String_{String_: value.ToString()},
		}, nil
	case querypb.Type_YEAR:
		i, err := value.ToInt64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Type_YEAR")
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Int{Int: int32(i)},
		}, nil
	case querypb.Type_CHAR, querypb.Type_ENUM, querypb.Type_SET, querypb.Type_GEOMETRY:
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_String_{String_: value.ToString()},
		}, nil

	case querypb.Type_BIT:
		b, err := value.ToBytes()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Type_BIT")
		}

		// Varint decodes an int64 from buf and returns that value and the
		// number of bytes read (> 0). If an error occurred, the value is 0
		// and the number of bytes n is <= 0 with the following meaning:
		//
		//	n == 0: buf too small
		//	n  < 0: value larger than 64 bits (overflow)
		//	        and -n is the number of bytes read
		i, n := binary.Varint(b)
		if n <= 0 {
			return nil, fmt.Errorf("failed to serialize Type_BIT, read %v bytes", n)
		}

		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Long{Long: i},
		}, nil
	}

	return nil, fmt.Errorf("unknown type %q", value.Type())
}
