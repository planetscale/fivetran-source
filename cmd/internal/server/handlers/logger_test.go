package handlers

import (
	"math"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	fivetransdk "github.com/planetscale/fivetran-proto/proto/fivetransdk/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestCanSerializeRecord(t *testing.T) {
	notes, err := sqltypes.NewValue(querypb.Type_TEXT, []byte("Something great comes this way"))
	require.NoError(t, err)
	decimal, err := sqltypes.NewValue(querypb.Type_DECIMAL, []byte("156.123"))
	require.NoError(t, err)
	profilePic, err := sqltypes.NewValue(querypb.Type_BINARY, []byte("profiles/phanatic.jpg"))
	require.NoError(t, err)
	siteMap, err := sqltypes.NewValue(querypb.Type_JSON, []byte("{'home': 'phanatic.dev'}"))
	require.NoError(t, err)
	floatValue, err := sqltypes.NewValue(querypb.Type_FLOAT32, []byte("123.456"))
	require.NoError(t, err)
	timestamp := "2006-01-02 15:04:05"
	row := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "customer_id",
				Type:         querypb.Type_INT32,
				ColumnLength: 21,
				Charset:      63,
				Flags:        32928,
			},
			{
				Name:         "name",
				Type:         querypb.Type_VARCHAR,
				ColumnLength: 21,
				Charset:      63,
				Flags:        32928,
			},
			{
				Name:  "is_deleted",
				Type:  querypb.Type_INT8,
				Flags: uint32(querypb.Flag_ISINTEGRAL),
			},
			{
				Name:    "notes",
				Type:    querypb.Type_TEXT,
				Charset: 63,
				Flags:   32928,
			},
			{
				Name:    "decimal",
				Type:    querypb.Type_DECIMAL,
				Charset: 63,
				Flags:   32928,
			},
			{
				Name:  "profile_pic",
				Type:  querypb.Type_BINARY,
				Flags: 32928,
			},
			{
				Name:  "sitemap",
				Type:  querypb.Type_JSON,
				Flags: 32928,
			},
			{
				Name:  "long_value",
				Type:  querypb.Type_INT64,
				Flags: 32928,
			},
			{
				Name:  "double_value",
				Type:  querypb.Type_FLOAT64,
				Flags: 32928,
			},
			{
				Name:  "float_value",
				Type:  querypb.Type_FLOAT32,
				Flags: 32928,
			},
			{
				Name:  "date_value",
				Type:  querypb.Type_DATE,
				Flags: 32928,
			},
			{
				Name:  "timestamp_value",
				Type:  querypb.Type_TIMESTAMP,
				Flags: 32928,
			},
			{
				Name:  "datetime_value",
				Type:  querypb.Type_DATETIME,
				Flags: 32928,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewInt32(123),
				sqltypes.NewVarChar("PhaniRaj"),
				sqltypes.NewInt8(0),
				notes,
				decimal,
				profilePic,
				siteMap,
				sqltypes.NewInt64(math.MaxInt64),
				sqltypes.NewFloat64(math.MaxFloat64),
				floatValue,
				sqltypes.NewDate("2004-12-12"),
				sqltypes.NewTimestamp(timestamp),
				sqltypes.NewDatetime("2021-01-19 03:14:07.999999"),
			},
		},
	}

	tl := &testLogSender{}
	l := NewLogger(tl, "", false)

	schema := &fivetransdk.SchemaSelection{
		SchemaName: "SalesDB",
	}
	table := &fivetransdk.TableSelection{
		TableName: "Customers",
	}

	err = l.Record(row, schema, table)
	assert.NoError(t, err)
	assert.NotNil(t, tl.lastResponse)

	operation, ok := tl.lastResponse.Response.(*fivetransdk.UpdateResponse_Operation)
	require.Truef(t, ok, "recordResponse Operation is not of type %s", "UpdateResponse_Operation")

	operationRecord, ok := operation.Operation.Op.(*fivetransdk.Operation_Record)
	assert.Truef(t, ok, "recordResponse Operation.Op is not of type %s", "Operation_Record")

	data := operationRecord.Record.Data
	assert.NotNil(t, data)

	assert.Equal(t, int32(123), data["customer_id"].GetShort())
	assert.Equal(t, "string:\"PhaniRaj\"", data["name"].String())
	assert.Equal(t, "string:\"Something great comes this way\"", data["notes"].String())
	assert.False(t, data["is_deleted"].GetBool())
	assert.Equal(t, "156.123", data["decimal"].GetDecimal())
	assert.Equal(t, []byte("profiles/phanatic.jpg"), data["profile_pic"].GetBinary())
	assert.Equal(t, "{'home': 'phanatic.dev'}", data["sitemap"].GetJson())
	assert.Equal(t, int64(math.MaxInt64), data["long_value"].GetLong())
	assert.Equal(t, math.MaxFloat64, data["double_value"].GetDouble())
	assert.Equal(t, float32(123.456), data["float_value"].GetFloat())
	dateValue := time.Date(2004, time.December, 12, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, timestamppb.New(dateValue), data["date_value"].GetNaiveDate())
	dts, err := time.Parse(timestamp, "2006-01-02 15:04:05")
	require.NoError(t, err)
	assert.Equal(t, timestamppb.New(dts), data["timestamp_value"].GetUtcDatetime())
	dt, err := time.Parse("2006-01-02 15:04:05", "2021-01-19 03:14:07.999999")
	require.NoError(t, err)
	assert.Equal(t, timestamppb.New(dt), data["datetime_value"].GetNaiveDatetime())
}

func BenchmarkRecordSerialization(b *testing.B) {
	row := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "customer_id",
				Type:         querypb.Type_VARCHAR,
				ColumnLength: 21,
				Charset:      63,
				Flags:        32928,
			},
			{
				Name: "is_deleted",
				Type: querypb.Type_BIT,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarChar("PhaniRaj"),
				sqltypes.TestValue(querypb.Type_BIT, "0"),
			},
		},
	}

	tl := &testLogSender{}
	l := NewLogger(tl, "", false)

	schema := &fivetransdk.SchemaSelection{
		SchemaName: "SalesDB",
	}
	table := &fivetransdk.TableSelection{
		TableName: "Customers",
	}

	for n := 0; n < b.N; n++ {
		l.Record(row, schema, table)
	}
}

func BenchmarkValueConversion(b *testing.B) {
	row := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "Customer_ID",
				Type:         querypb.Type_UINT64,
				ColumnLength: 21,
				Charset:      63,
				Flags:        32928,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewInt32(1),
				sqltypes.TestValue(sqltypes.Float32, "1.1"),
				sqltypes.NewVarChar("value1"),
				sqltypes.TestValue(sqltypes.Decimal, "123.45"),
				sqltypes.TestValue(sqltypes.Uint32, "2147483647"),
				sqltypes.NewUint64(9223372036854775807),
			},
		},
	}

	tl := &testLogSender{}
	l := NewLogger(tl, "", false)

	schema := &fivetransdk.SchemaSelection{
		SchemaName: "SalesDB",
	}
	table := &fivetransdk.TableSelection{
		TableName: "Customers",
	}

	for n := 0; n < b.N; n++ {
		l.Record(row, schema, table)
	}
}
