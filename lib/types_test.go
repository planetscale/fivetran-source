package lib

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializedCursorSuspectedBinlogExpirationIsSeparateField(t *testing.T) {
	// State written by fa7fb45 (with or without the #73 marker) parses unchanged
	// and is not treated as suspected.
	for _, old := range []string{
		`{"cursor":"abc"}`,
		`{"cursor":"abc","error_message":"Binlogs have expired.","error_code":"BINLOG_EXPIRATION_ERROR"}`,
	} {
		var sc SerializedCursor
		assert.NoError(t, json.Unmarshal([]byte(old), &sc))
		assert.False(t, sc.HasSuspectedBinlogExpiration())
		out, err := json.Marshal(sc)
		assert.NoError(t, err)
		assert.JSONEq(t, old, string(out), "an unmarked cursor serializes exactly as before")
	}

	// Marking only sets the new field, never error_code/error_message.
	sc := SerializedCursor{Cursor: "abc"}
	sc.SetSuspectedBinlogExpiration()
	assert.Nil(t, sc.ErrorCode)
	assert.Nil(t, sc.ErrorMessage)
	out, err := json.Marshal(sc)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"cursor":"abc","suspected_binlog_expiration":true}`, string(out))

	// Clearing only clears the new field, and leaves a #73 marker alone.
	sc.SetBinlogExpirationError("Binlogs have expired.")
	sc.ClearSuspectedBinlogExpiration()
	assert.False(t, sc.HasSuspectedBinlogExpiration())
	if assert.NotNil(t, sc.ErrorCode) {
		assert.Equal(t, "BINLOG_EXPIRATION_ERROR", *sc.ErrorCode)
	}

	// The #73 marker alone is not a suspected expiration.
	only73 := SerializedCursor{Cursor: "abc"}
	only73.SetBinlogExpirationError("Binlogs have expired.")
	assert.False(t, only73.HasSuspectedBinlogExpiration())
}

func shardStatesWith(shards ...string) ShardStates {
	ss := ShardStates{Shards: map[string]*SerializedCursor{}}
	for _, s := range shards {
		ss.Shards[s] = &SerializedCursor{}
	}
	return ss
}

// Old state has only the table-level order: every shard is compared with it.
func TestSavedColumnOrderFallsBackToTableLevel(t *testing.T) {
	ss := shardStatesWith("-80", "80-")
	ss.ColumnOrder = []string{"id", "a"}
	assert.Equal(t, []string{"id", "a"}, ss.SavedColumnOrder("-80"))
	assert.Equal(t, []string{"id", "a"}, ss.SavedColumnOrder("80-"))

	ss.ShardColumnOrders = map[string][]string{"-80": {"id", "x", "a"}}
	assert.Equal(t, []string{"id", "x", "a"}, ss.SavedColumnOrder("-80"))
	assert.Equal(t, []string{"id", "a"}, ss.SavedColumnOrder("80-"))

	assert.Nil(t, shardStatesWith("-").SavedColumnOrder("-"))
}

// Recording one shard never moves another shard's saved order: shards without
// an entry first get the table-level order they were compared with, and the
// table-level order only follows once every shard agrees.
func TestRecordShardColumnOrder(t *testing.T) {
	old, cut := []string{"id", "a"}, []string{"id", "x", "a"}
	ss := shardStatesWith("-80", "80-")
	ss.ColumnOrder = old

	ss.RecordShardColumnOrder("-80", cut)
	assert.Equal(t, map[string][]string{"-80": cut, "80-": old}, ss.ShardColumnOrders)
	assert.Equal(t, old, ss.ColumnOrder, "the shards disagree: the table-level order stays behind")

	ss.RecordShardColumnOrder("80-", cut)
	assert.Equal(t, cut, ss.ColumnOrder, "every shard has the new order")

	// Unsharded: one entry, and the table-level order follows it at once.
	un := shardStatesWith("-")
	un.RecordShardColumnOrder("-", cut)
	assert.Equal(t, map[string][]string{"-": cut}, un.ShardColumnOrders)
	assert.Equal(t, cut, un.ColumnOrder)

	// No table-level order yet (a copy from scratch): it is set only once
	// every shard has an entry.
	fresh := shardStatesWith("-80", "80-")
	fresh.RecordShardColumnOrder("-80", cut)
	assert.Nil(t, fresh.ColumnOrder)
	fresh.RecordShardColumnOrder("80-", cut)
	assert.Equal(t, cut, fresh.ColumnOrder)
}

// State round trip with a connector version that only knows the table-level
// order: it reads column_order, which is never ahead of any shard (here the
// order before the cut-over, so it checks again), and state it writes back
// (without shard_column_orders) is read by this version through the
// table-level fallback.
func TestColumnOrderStateRollbackRoundTrip(t *testing.T) {
	old, cut := []string{"id", "a"}, []string{"id", "x", "a"}
	ss := shardStatesWith("-80", "80-")
	ss.ColumnOrder = old
	ss.RecordShardColumnOrder("-80", cut)
	raw, err := json.Marshal(ss)
	assert.NoError(t, err)
	assert.Contains(t, string(raw), `"shard_column_orders"`)

	type tableLevelOnly struct {
		Shards      map[string]*SerializedCursor `json:"shards"`
		ColumnOrder []string                     `json:"column_order,omitempty"`
	}
	var older tableLevelOnly
	assert.NoError(t, json.Unmarshal(raw, &older))
	assert.Equal(t, old, older.ColumnOrder)
	assert.Len(t, older.Shards, 2)

	rolledBack, err := json.Marshal(older)
	assert.NoError(t, err)
	var back ShardStates
	assert.NoError(t, json.Unmarshal(rolledBack, &back))
	assert.Empty(t, back.ShardColumnOrders)
	assert.Equal(t, old, back.SavedColumnOrder("-80"))
	assert.Equal(t, old, back.SavedColumnOrder("80-"))

	// Without any column order, the field is left out as before.
	raw, err = json.Marshal(shardStatesWith("-"))
	assert.NoError(t, err)
	assert.NotContains(t, string(raw), "column_order")
}
