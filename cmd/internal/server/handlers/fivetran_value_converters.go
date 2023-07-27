package handlers

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/pkg/errors"
	fivetransdk_v2 "github.com/planetscale/fivetran-sdk-grpc/go"
	"google.golang.org/protobuf/types/known/timestamppb"

	"vitess.io/vitess/go/sqltypes"
)

type ConverterFunc func(sqltypes.Value) (*fivetransdk_v2.ValueType, error)

var converters = map[fivetransdk_v2.DataType]ConverterFunc{
	fivetransdk_v2.DataType_STRING: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		return &fivetransdk_v2.ValueType{
			Inner: &fivetransdk_v2.ValueType_String_{String_: value.ToString()},
		}, nil
	},
	fivetransdk_v2.DataType_BOOLEAN: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		b, err := value.ToBool()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_BOOLEAN")
		}
		return &fivetransdk_v2.ValueType{Inner: &fivetransdk_v2.ValueType_Bool{Bool: b}}, nil
	},
	fivetransdk_v2.DataType_SHORT: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		i, err := value.ToInt64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_SHORT")
		}

		if i > math.MaxInt32 {
			return nil, errors.Wrap(err, "Int32 value will overflow")
		}

		return &fivetransdk_v2.ValueType{
			Inner: &fivetransdk_v2.ValueType_Short{Short: int32(i)},
		}, nil
	},
	fivetransdk_v2.DataType_INT: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		i, err := value.ToInt64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_INT")
		}
		if i > math.MaxInt32 {
			return nil, errors.Wrap(err, "Int32 value will overflow")
		}
		return &fivetransdk_v2.ValueType{
			Inner: &fivetransdk_v2.ValueType_Int{Int: int32(i)},
		}, nil
	},
	fivetransdk_v2.DataType_LONG: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		if value.IsIntegral() {
			i, err := value.ToInt64()
			if err != nil {
				return nil, errors.Wrap(err, "failed to serialize DataType_LONG")
			}

			return &fivetransdk_v2.ValueType{Inner: &fivetransdk_v2.ValueType_Long{Long: i}}, nil
		}
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
			return nil, fmt.Errorf("failed to serialize DataType_LONG, read %v bytes", n)
		}

		return &fivetransdk_v2.ValueType{
			Inner: &fivetransdk_v2.ValueType_Long{Long: i},
		}, nil
	},
	fivetransdk_v2.DataType_DECIMAL: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		return &fivetransdk_v2.ValueType{
			Inner: &fivetransdk_v2.ValueType_Decimal{Decimal: value.ToString()},
		}, nil
	},

	fivetransdk_v2.DataType_FLOAT: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		f, err := value.ToFloat64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_FLOAT")
		}
		if f > math.MaxFloat32 {
			return nil, errors.Wrap(err, "Float32 value will overflow")
		}
		return &fivetransdk_v2.ValueType{
			Inner: &fivetransdk_v2.ValueType_Float{Float: float32(f)},
		}, nil
	},
	fivetransdk_v2.DataType_DOUBLE: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		f, err := value.ToFloat64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_DOUBLE")
		}

		return &fivetransdk_v2.ValueType{
			Inner: &fivetransdk_v2.ValueType_Double{Double: f},
		}, nil
	},

	fivetransdk_v2.DataType_NAIVE_DATE: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		// The DATE type is used for values with a date part but no time part.
		// MySQL retrieves and displays DATE values in 'YYYY-MM-DD' format.
		// The supported range is '1000-01-01' to '9999-12-31'.
		t, err := time.Parse("2006-01-02", value.ToString())
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_NAIVE_DATE")
		}
		return &fivetransdk_v2.ValueType{
			Inner: &fivetransdk_v2.ValueType_NaiveDate{NaiveDate: timestamppb.New(t)},
		}, nil
	},
	fivetransdk_v2.DataType_NAIVE_DATETIME: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		// The DATETIME type is used for values that contain both date and time parts.
		// MySQL retrieves and displays DATETIME values in 'YYYY-MM-DD hh:mm:ss' format.
		// The supported range is '1000-01-01 00:00:00' to '9999-12-31 23:59:59'.
		t, err := time.Parse("2006-01-02 15:04:05", value.ToString())
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_NAIVE_DATETIME")
		}
		return &fivetransdk_v2.ValueType{
			Inner: &fivetransdk_v2.ValueType_NaiveDatetime{NaiveDatetime: timestamppb.New(t)},
		}, nil
	},
	fivetransdk_v2.DataType_UTC_DATETIME: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		// The TIMESTAMP data type is used for values that contain both date and time parts.
		// TIMESTAMP has a range of '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.
		t, err := time.Parse("2006-01-02 15:04:05", value.ToString())
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_UTC_DATETIME")
		}
		return &fivetransdk_v2.ValueType{
			Inner: &fivetransdk_v2.ValueType_UtcDatetime{UtcDatetime: timestamppb.New(t)},
		}, nil
	},
	fivetransdk_v2.DataType_BINARY: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		b, err := value.ToBytes()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_BINARY")
		}
		return &fivetransdk_v2.ValueType{
			Inner: &fivetransdk_v2.ValueType_Binary{Binary: b},
		}, nil
	},
	fivetransdk_v2.DataType_JSON: func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
		return &fivetransdk_v2.ValueType{
			Inner: &fivetransdk_v2.ValueType_Json{Json: value.ToString()},
		}, nil
	},
}

var convertTinyIntToBool = func(value sqltypes.Value) (*fivetransdk_v2.ValueType, error) {
	b, err := value.ToBool()
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize Type_INT8")
	}
	return &fivetransdk_v2.ValueType{
		Inner: &fivetransdk_v2.ValueType_Bool{Bool: b},
	}, nil
}

func GetConverter(dataType fivetransdk_v2.DataType, serializeTinyIntAsBool bool) (ConverterFunc, error) {
	if serializeTinyIntAsBool && dataType == fivetransdk_v2.DataType_INT {
		return convertTinyIntToBool, nil
	}

	converter, ok := converters[dataType]
	if !ok {
		return nil, fmt.Errorf("don't know how to convert type %s", dataType)
	}
	return converter, nil
}
