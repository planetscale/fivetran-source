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
