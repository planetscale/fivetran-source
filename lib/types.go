package lib

import (
	"encoding/base64"
	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/core/codec"
)

type MysqlColumn struct {
	Name         string
	Type         string
	IsPrimaryKey bool
}

type SchemaBuilder interface {
	OnKesypace(keyspaceName string)
	OnTable(keyspaceName, tableName string)
	OnColumn(keyspaceName, tableName, columnName, mysqlType string, isPrimaryKey bool)
}

// ConfiguredRequest is a grpc request that contains a Configuration in the payload.
// current examples are : Test, Schema & Update
type ConfiguredRequest interface {
	GetConfiguration() map[string]string
}

type StatefulRequest interface {
	GetStateJson() string
}

// SourceFromRequest extracts the required configuration values from the map
// and returns a usable PlanetScaleSource to connect to a PlanetScale database.
func SourceFromRequest(request ConfiguredRequest) (*PlanetScaleSource, error) {
	psc := &PlanetScaleSource{}
	configuration := request.GetConfiguration()
	if val, ok := configuration["username"]; ok {
		psc.Username = val
	} else {
		return nil, errors.New("username not found in configuration")
	}

	if val, ok := configuration["password"]; ok {
		psc.Password = val
	} else {
		return nil, errors.New("password not found in configuration")
	}

	if val, ok := configuration["database"]; ok {
		psc.Database = val
	} else {
		return nil, errors.New("database not found in configuration")
	}

	if val, ok := configuration["host"]; ok {
		psc.Host = val
	} else {
		return nil, errors.New("hostname not found in configuration")
	}

	return psc, nil
}

func (s SerializedCursor) SerializedCursorToTableCursor() (*psdbconnect.TableCursor, error) {
	var tc psdbconnect.TableCursor
	decoded, err := base64.StdEncoding.DecodeString(s.Cursor)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decode table cursor")
	}

	err = codec.DefaultCodec.Unmarshal(decoded, &tc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to deserialize table cursor")
	}

	return &tc, nil
}

func TableCursorToSerializedCursor(cursor *psdbconnect.TableCursor) (*SerializedCursor, error) {
	d, err := codec.DefaultCodec.Marshal(cursor)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal table cursor to save staate.")
	}

	sc := &SerializedCursor{
		Cursor: base64.StdEncoding.EncodeToString(d),
	}
	return sc, nil
}

type SerializedCursor struct {
	Cursor string `json:"cursor"`
}

type ShardStates struct {
	Shards map[string]*SerializedCursor `json:"shards"`
}

type KeyspaceState struct {
	Streams map[string]ShardStates `json:"streams"`
}

type SyncState struct {
	Keyspaces map[string]KeyspaceState `json:"keyspaces"`
}
