package lib

import (
	"encoding/base64"
	"slices"

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
	// SchemaScan is how far the schema history scan of this cursor got (see
	// SchemaScanProgress), so the next scan of the same cursor resumes there.
	// It lives on the cursor because it only describes this cursor: a new one
	// (the shard's read moved on, or a reset) starts without it.
	SchemaScan *SchemaScanProgress `json:"schema_scan,omitempty"`
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
	// ShardColumnOrders is, per shard, every column of the source table (not
	// only the columns selected in Fivetran) in table order, as that shard had
	// it when its cursor was last read up to the current position. The next
	// sync compares each shard's entry with that shard's live order
	// (ColumnOrderChange) to catch a column added in the middle of the table,
	// or columns reordered, after the shard's saved cursor. It is per shard
	// because Online DDL cuts every shard over on its own: during a schema
	// change the shards of a table can be at different column orders.
	ShardColumnOrders map[string][]string `json:"shard_column_orders,omitempty"`
	// ColumnOrder is the table-level column order: written when every shard
	// with an entry in ShardColumnOrders has the same order (always, for an
	// unsharded table), and left as it was while the shards disagree, so it
	// is never ahead of any shard's cursor. A shard without an entry in
	// ShardColumnOrders (state from a connector version before per-shard
	// orders) is compared with it. Kept so that state stays usable by a
	// connector version that only knows the table-level order. State with
	// neither (older connector versions, or a "Resync all data", which starts
	// from empty state) skips the comparison and records the order; older
	// versions ignore both fields.
	ColumnOrder []string `json:"column_order,omitempty"`
}

// SavedColumnOrder returns the column order saved for shard: its own entry in
// ShardColumnOrders, or else the table-level ColumnOrder (nil if neither).
func (s ShardStates) SavedColumnOrder(shard string) []string {
	if order, ok := s.ShardColumnOrders[shard]; ok && len(order) > 0 {
		return order
	}
	return s.ColumnOrder
}

// RecordShardColumnOrder saves order as shard's column order. A shard of the
// table that has no entry yet first gets the table-level order it was
// compared with until now, so changing the table-level order never moves
// another shard's saved order. The table-level ColumnOrder is then set to the
// common order if every shard of the table has the same one, and left as it
// was while they disagree.
func (s *ShardStates) RecordShardColumnOrder(shard string, order []string) {
	if s.ShardColumnOrders == nil {
		s.ShardColumnOrders = map[string][]string{}
	}
	for sh := range s.Shards {
		if _, ok := s.ShardColumnOrders[sh]; !ok && len(s.ColumnOrder) > 0 {
			s.ShardColumnOrders[sh] = slices.Clone(s.ColumnOrder)
		}
	}
	s.ShardColumnOrders[shard] = slices.Clone(order)
	var common []string
	for sh := range s.Shards {
		o, ok := s.ShardColumnOrders[sh]
		if !ok || (common != nil && !slices.Equal(common, o)) {
			return
		}
		common = o
	}
	if common != nil {
		s.ColumnOrder = slices.Clone(common)
	}
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
