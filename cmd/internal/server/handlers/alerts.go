package handlers

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

// maxAlertItems caps how many tables/shards are listed in one alert message.
const maxAlertItems = 20

// binlogExpirationSet de-duplicates "keyspace.table (shard s)" entries for the
// combined binlog-expiration Warning.
type binlogExpirationSet struct {
	seen  map[string]struct{}
	items []string
}

func (b *binlogExpirationSet) add(keyspace, table, shard string) {
	item := fmt.Sprintf("%s.%s (shard %s)", keyspace, table, shard)
	if b.seen == nil {
		b.seen = map[string]struct{}{}
	}
	if _, ok := b.seen[item]; ok {
		return
	}
	b.seen[item] = struct{}{}
	b.items = append(b.items, item)
}

func (b *binlogExpirationSet) warningMessage() string {
	if len(b.items) == 0 {
		return ""
	}
	return "PlanetScale binary logs expired before Fivetran could read the latest changes for the following table(s): " +
		joinLimited(b.items) + ". " +
		"An automatic historical re-sync has been initiated. No action is needed."
}

// StaleShardsWarningMessage builds the customer-facing Warning sent when stale
// shard entries written by an older connector version are dropped from state.
// prunedByKeyspace maps keyspace -> pruned shard names.
func StaleShardsWarningMessage(prunedByKeyspace map[string][]string) string {
	if len(prunedByKeyspace) == 0 {
		return ""
	}
	keyspaces := make([]string, 0, len(prunedByKeyspace))
	for ks := range prunedByKeyspace {
		keyspaces = append(keyspaces, ks)
	}
	sort.Strings(keyspaces)
	// The cap applies to the total number of shards listed, across keyspaces.
	var items []string
	listed, total := 0, 0
	for _, ks := range keyspaces {
		shards := append([]string(nil), prunedByKeyspace[ks]...)
		sort.Strings(shards)
		total += len(shards)
		if room := maxAlertItems - listed; room > 0 {
			if len(shards) > room {
				shards = shards[:room]
			}
			listed += len(shards)
			items = append(items, fmt.Sprintf("keyspace %s: %s", ks, strings.Join(shards, ", ")))
		}
	}
	list := strings.Join(items, ", ")
	if total > listed {
		list += fmt.Sprintf(" and %d more", total-listed)
	}
	return "PlanetScale removed stale shard references left in this connection's sync state by an older connector version (" +
		list + "). " +
		"These references never contained data. No action or re-sync is needed."
}

func joinLimited(items []string) string {
	if len(items) <= maxAlertItems {
		return strings.Join(items, ", ")
	}
	return strings.Join(items[:maxAlertItems], ", ") + fmt.Sprintf(" and %d more", len(items)-maxAlertItems)
}

// serializationTaskMessage returns the Task text for a source value that can't
// be converted for Fivetran, or "" if err is not a *RowSerializationError.
// Neither the value nor the primary key values appear in it (possible PII);
// the primary key goes to the connector log instead (serializationLogMessage).
// It only depends on the schema, so Fivetran's de-duplication works.
func serializationTaskMessage(err error, keyspace, table string, treatTinyIntAsBoolean bool) string {
	var rse *RowSerializationError
	if !errors.As(err, &rse) {
		return ""
	}
	msg := fmt.Sprintf("Sync stopped: column %q (MySQL type %s) of table %q in PlanetScale keyspace %q has a value that can't be converted for Fivetran. "+
		"Find values that are incompatible with the datatype.",
		rse.Column, rse.ColumnType, table, keyspace)
	if treatTinyIntAsBoolean && strings.HasPrefix(strings.ToLower(rse.ColumnType), "tinyint(1)") {
		msg += " If this TINYINT(1) column intentionally stores values other than 0 and 1, turn off \"Treat tinyint(1) as boolean\" " +
			"in the connection setup instead, then click \"Resync all data\" on the connection's Settings tab."
	}
	return msg
}

// serializationLogMessage is the connector log line for a value that can't be
// converted. It includes the primary key column=value pairs so Fivetran support
// can point the customer at the row, but never the offending value.
func serializationLogMessage(rse *RowSerializationError, keyspace, table string) string {
	pk := rse.PrimaryKey
	if pk == "" {
		pk = "none (table has no primary key)"
	}
	return fmt.Sprintf("value that can't be converted for Fivetran: keyspace %q, table %q, column %q (MySQL type %s), primary key [%s]; the value itself is not logged",
		keyspace, table, rse.Column, rse.ColumnType, pk)
}
