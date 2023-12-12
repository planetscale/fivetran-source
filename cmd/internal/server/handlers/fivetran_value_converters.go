package handlers

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/spatial-go/geoos/geoencoding"
	"github.com/spatial-go/geoos/geoencoding/geojson"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"github.com/pkg/errors"
	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk"
	"google.golang.org/protobuf/types/known/timestamppb"
	"vitess.io/vitess/go/sqltypes"
)

type ConverterFunc func(sqltypes.Value) (*fivetransdk.ValueType, error)

var converters = map[fivetransdk.DataType]ConverterFunc{
	fivetransdk.DataType_STRING: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_String_{String_: value.ToString()},
		}, nil
	},
	fivetransdk.DataType_BOOLEAN: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		b, err := value.ToBool()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_BOOLEAN")
		}
		return &fivetransdk.ValueType{Inner: &fivetransdk.ValueType_Bool{Bool: b}}, nil
	},
	fivetransdk.DataType_SHORT: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		i, err := value.ToInt64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_SHORT")
		}

		if i > math.MaxInt32 {
			return nil, errors.Wrap(err, "Int32 value will overflow")
		}

		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Short{Short: int32(i)},
		}, nil
	},
	fivetransdk.DataType_INT: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		i, err := value.ToInt64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_INT")
		}
		if i > math.MaxInt32 {
			return nil, errors.Wrap(err, "Int32 value will overflow")
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Int{Int: int32(i)},
		}, nil
	},
	fivetransdk.DataType_LONG: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		if value.IsIntegral() {
			i, err := value.ToInt64()
			if err != nil {
				return nil, errors.Wrap(err, "failed to serialize DataType_LONG")
			}

			return &fivetransdk.ValueType{Inner: &fivetransdk.ValueType_Long{Long: i}}, nil
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

		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Long{Long: i},
		}, nil
	},
	fivetransdk.DataType_DECIMAL: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Decimal{Decimal: value.ToString()},
		}, nil
	},

	fivetransdk.DataType_FLOAT: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		f, err := value.ToFloat64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_FLOAT")
		}
		if f > math.MaxFloat32 {
			return nil, errors.Wrap(err, "Float32 value will overflow")
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Float{Float: float32(f)},
		}, nil
	},
	fivetransdk.DataType_DOUBLE: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		f, err := value.ToFloat64()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_DOUBLE")
		}

		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Double{Double: f},
		}, nil
	},

	fivetransdk.DataType_NAIVE_DATE: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		// The DATE type is used for values with a date part but no time part.
		// MySQL retrieves and displays DATE values in 'YYYY-MM-DD' format.
		// The supported range is '1000-01-01' to '9999-12-31'.
		t, err := time.Parse(time.DateOnly, value.ToString())
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_NAIVE_DATE")
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_NaiveDate{NaiveDate: timestamppb.New(t)},
		}, nil
	},
	fivetransdk.DataType_NAIVE_DATETIME: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		// The DATETIME type is used for values that contain both date and time parts.
		// MySQL retrieves and displays DATETIME values in 'YYYY-MM-DD hh:mm:ss' format.
		// The supported range is '1000-01-01 00:00:00' to '9999-12-31 23:59:59'.

		t, err := time.Parse(time.DateTime, value.ToString())
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_NAIVE_DATETIME")
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_NaiveDatetime{NaiveDatetime: timestamppb.New(t)},
		}, nil
	},
	fivetransdk.DataType_UTC_DATETIME: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		// The TIMESTAMP data type is used for values that contain both date and time parts.
		// TIMESTAMP has a range of '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.
		t, err := time.Parse("2006-01-02 15:04:05", value.ToString())
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_UTC_DATETIME")
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_UtcDatetime{UtcDatetime: timestamppb.New(t)},
		}, nil
	},
	fivetransdk.DataType_BINARY: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		b, err := value.ToBytes()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize DataType_BINARY")
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Binary{Binary: b},
		}, nil
	},
	fivetransdk.DataType_JSON: func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		if value.Type() == querypb.Type_GEOMETRY {
			// we tell Fivetran that all geoemtry types are in fact serialized as JSON values.
			// which is why we need to depend on the value type here to inform us to serialize
			// geometry values as GeoJson
			// 1. Get the Well Known Binary representation of the geometry value here.
			// Docs: https://dev.mysql.com/doc/refman/8.0/en/gis-data-formats.html#gis-wkb-format
			b, err := value.ToBytes()
			if err != nil {
				return nil, errors.Wrap(err, "unable to get bytes for Geometry type")
			}

			// vitess's value types return the WKB as an array of bytes in base 10
			// we use this function to get the hex representation of the base-10 WKB
			hexValues := fmt.Sprintf("%x", b)
			buf := new(bytes.Buffer)
			// Skip the first 8 characters because they're padding
			// as mysql uses 4 bytes to store the WKB of geometry types.
			// Since WKB in mysql only uses 25 chracters, where the 1st character
			// determines Endianness, and is used by vitess when we call `ToBytes` above.
			buf.Write([]byte(hexValues[8:]))
			got, err := geoencoding.Read(buf, geoencoding.WKB)
			if err != nil {
				return nil, errors.Wrap(err, "unable to serialize Geometry type")
			}
			if got == nil {
				return nil, fmt.Errorf("invalid Geometry value: %s", b)
			}

			// serialize the geometric shape as GeoJson
			// GeoJson examples are available in the RFC here :
			// https://datatracker.ietf.org/doc/html/rfc7946#section-1.5
			gj := geojson.GeojsonEncoder{}
			geoJson := gj.Encode(got.Geom())
			return &fivetransdk.ValueType{
				Inner: &fivetransdk.ValueType_Json{Json: string(geoJson)},
			}, nil
		}
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Json{Json: value.ToString()},
		}, nil
	},
}

func GetConverter(dataType fivetransdk.DataType) (ConverterFunc, error) {
	converter, ok := converters[dataType]
	if !ok {
		return nil, fmt.Errorf("don't know how to convert type %s", dataType)
	}
	return converter, nil
}

func GetEnumConverter(enumValues []string) (ConverterFunc, error) {
	return func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		parsedValue := value.ToString()
		index, err := strconv.ParseInt(parsedValue, 10, 64)
		if err != nil {
			// If value is not an integer (index), we just serialize it as a string
			return &fivetransdk.ValueType{
				Inner: &fivetransdk.ValueType_String_{String_: parsedValue},
			}, nil
		}

		// The index value of the empty string error value is 0
		if index == 0 {
			return &fivetransdk.ValueType{
				Inner: &fivetransdk.ValueType_String_{String_: ""},
			}, nil
		}

		for i, v := range enumValues {
			if int(index-1) == i {
				return &fivetransdk.ValueType{
					Inner: &fivetransdk.ValueType_String_{String_: v},
				}, nil
			}
		}

		// Just return the value as a string if we can't find the enum value
		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_String_{String_: parsedValue},
		}, nil
	}, nil
}

func GetSetConverter(setValues []string) (ConverterFunc, error) {
	return func(value sqltypes.Value) (*fivetransdk.ValueType, error) {
		parsedValue := value.ToString()
		parsedInt, err := strconv.ParseInt(parsedValue, 10, 64)
		if err != nil {
			// if value is not an integer, we just serialize as a strong
			return &fivetransdk.ValueType{
				Inner: &fivetransdk.ValueType_Json{Json: parsedValue},
			}, nil
		}
		mappedValues := []string{}
		// SET mapping is stored as a binary value, i.e. 1001
		bytes := strconv.FormatInt(parsedInt, 2)
		numValues := len(bytes)
		// if the bit is ON, that means the value at that index is included in the SET
		for i, char := range bytes {
			if char == '1' {
				// bytes are in reverse order, the first bit represents the last value in the SET
				mappedValue := setValues[numValues-(i+1)]
				mappedValues = append([]string{mappedValue}, mappedValues...)
			}
		}

		// If we can't find the values, just serialize as a string
		if len(mappedValues) == 0 {
			return &fivetransdk.ValueType{
				Inner: &fivetransdk.ValueType_Json{Json: parsedValue},
			}, nil
		}

		return &fivetransdk.ValueType{
			Inner: &fivetransdk.ValueType_Json{Json: strings.Join(mappedValues, ",")},
		}, nil
	}, nil
}
