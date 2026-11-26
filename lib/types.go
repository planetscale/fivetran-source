package lib

import (
	"encoding/base64"

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
	Cursor       string  `json:"cursor"`
	ErrorMessage *string `json:"error_message,omitempty"`
	ErrorCode    *string `json:"error_code,omitempty"`
	// SuspectedBinlogExpiration is set when the last sync of this shard cursor
	// failed with the masked "failed to find a <TYPE> tablet" form of a
	// purged-binlog error (see SuspectedBinlogExpirationError). If the next sync
	// fails the same way, a "Re-sync required" Task is raised; the cursor itself
	// is never reset. It is independent of ErrorCode/ErrorMessage, which are only
	// used by the explicit binlog-expiration reset (SetBinlogExpirationError).
	// Old state without the field parses as false, and older connector versions
	// ignore it.
	SuspectedBinlogExpiration bool `json:"suspected_binlog_expiration,omitempty"`
}

// SetBinlogExpirationError sets a binlog expiration error on the cursor
func (sc *SerializedCursor) SetBinlogExpirationError(errorMessage string) {
	sc.ErrorCode = stringPtr("BINLOG_EXPIRATION_ERROR")
	sc.ErrorMessage = &errorMessage
}

// SetSuspectedBinlogExpiration marks the cursor as having failed once with a
// suspected binlog expiration. It only touches SuspectedBinlogExpiration.
func (sc *SerializedCursor) SetSuspectedBinlogExpiration() {
	sc.SuspectedBinlogExpiration = true
}

// HasSuspectedBinlogExpiration reports whether the previous sync of this cursor
// failed with a suspected binlog expiration. It only reads
// SuspectedBinlogExpiration.
func (sc *SerializedCursor) HasSuspectedBinlogExpiration() bool {
	return sc != nil && sc.SuspectedBinlogExpiration
}

// ClearSuspectedBinlogExpiration removes the mark. It only touches
// SuspectedBinlogExpiration.
func (sc *SerializedCursor) ClearSuspectedBinlogExpiration() {
	sc.SuspectedBinlogExpiration = false
}

func stringPtr(s string) *string {
	return &s
}

type ShardStates struct {
	Shards map[string]*SerializedCursor `json:"shards"`
	// ColumnOrder is every column of the source table (not only the columns
	// selected in Fivetran), in table order, as of the last sync that read the
	// table. The next sync compares it with the live order (ColumnOrderChange)
	// to catch a column added in the middle of the table, or columns
	// reordered, after the saved cursors. It is per table: every shard has the
	// same schema. State without the field (older connector versions, or a
	// "Resync all data", which starts from empty state) skips the comparison
	// and records the order; older versions ignore the field.
	ColumnOrder []string `json:"column_order,omitempty"`
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
