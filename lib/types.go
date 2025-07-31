package lib

import (
	"encoding/base64"
	"strings"

	"vitess.io/vitess/go/sqltypes"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/core/codec"
)

type Operation int64

const (
	OpType_Insert Operation = iota
	OpType_Update
	OpType_Delete
	OpType_Truncate
)

type UpdatedRow struct {
	Before *sqltypes.Result
	After  *sqltypes.Result
}
type MysqlColumn struct {
	Name         string
	Type         string
	IsPrimaryKey bool
}

type SchemaBuilder interface {
	OnKeyspace(keyspaceName string)
	OnTable(keyspaceName, tableName string)
	OnColumns(keyspaceName, tableName string, columns []MysqlColumn)
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

// A map of starting GTIDs for every keyspace and shard
// i.e. { keyspace: { shard: gtid} }
type StartingGtids map[string]map[string]string

var BinLogsExpirationError = errors.New("Historical sync required due to binlog expiration")

func IsBinlogsExpirationError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Cannot replicate because the source purged required binary logs")
}
